/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientException;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getSslSettingsBuilder;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/initial-dns-seedlist-discovery/tests
@RunWith(Parameterized.class)
public class InitialDnsSeedlistDiscoveryTest {

    private final String filename;
    private final String uri;
    private final List<String> seeds;
    private final List<ServerAddress> hosts;
    private final BsonDocument options;

    public InitialDnsSeedlistDiscoveryTest(final String filename, final String uri, final List<String> seeds,
                                           final List<ServerAddress> hosts, final BsonDocument options) {
        this.filename = filename;
        this.uri = uri;
        this.seeds = seeds;
        this.hosts = hosts;
        this.options = options;
    }

    @Test
    public void shouldResolve() {
        ConnectionString connectionString = new ConnectionString(this.uri);

        if (seeds.isEmpty()) {
            try {
                connectionString.applyDirectoryResolution(createDnsDirContext());
                fail();
            } catch (MongoClientException e) {
                assertEquals(NameNotFoundException.class, e.getCause().getClass());
            }
        } else {
            ConnectionString resolvedConnectionString = connectionString.applyDirectoryResolution(createDnsDirContext());

            assertEquals(seeds.size(), resolvedConnectionString.getHosts().size());
            assertTrue(resolvedConnectionString.getHosts().containsAll(seeds));

            for (Map.Entry<String, BsonValue> entry : options.entrySet()) {
                if (entry.getKey().equals("connectTimeoutMS")) {
                    assertEquals(entry.getValue().asNumber().intValue(), (int) resolvedConnectionString.getConnectTimeout());
                } else if (entry.getKey().equals("replicaSet")) {
                    assertEquals(entry.getValue().asString().getValue(), resolvedConnectionString.getRequiredReplicaSetName());
                } else if (entry.getKey().equals("socketTimeoutMS")) {
                    assertEquals(entry.getValue().asNumber().intValue(), (int) resolvedConnectionString.getSocketTimeout());
                } else {
                    throw new UnsupportedOperationException("No support configured yet for " + entry.getKey());
                }
            }
        }
    }

    @Test
    public void shouldDiscover() throws InterruptedException {
        assumeTrue(!seeds.isEmpty());
        assumeTrue(isDiscoverableReplicaSet());

        final CountDownLatch latch = new CountDownLatch(1);

        MongoClient client = MongoClients.create(new ConnectionString(uri),
                ClusterSettings.builder().addClusterListener(new ClusterListener() {
                    @Override
                    public void clusterOpening(final ClusterOpeningEvent event) {
                    }

                    @Override
                    public void clusterClosed(final ClusterClosedEvent event) {
                    }

                    @Override
                    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
                        List<ServerAddress> curHostList = new ArrayList<ServerAddress>();
                        for (ServerDescription cur : event.getNewDescription().getServerDescriptions()) {
                            if (cur.isOk()) {
                                curHostList.add(cur.getAddress());
                            }
                        }
                        if (hosts.size() == curHostList.size() && curHostList.containsAll(hosts)) {
                            latch.countDown();
                        }

                    }
                }), ConnectionPoolSettings.builder(), ServerSettings.builder(),
                getSslSettingsBuilder(),
                SocketSettings.builder());

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        final CountDownLatch pingLatch = new CountDownLatch(1);
        client.getDatabase("admin").runCommand(new Document("ping", 1), new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final Throwable t) {
                if (t == null) {
                    pingLatch.countDown();
                }
            }
        });

        assertTrue(pingLatch.await(5, TimeUnit.SECONDS));

        client.close();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/initial-dns-seedlist-discovery")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            data.add(new Object[]{
                    file.getName(),
                    testDocument.getString("uri").getValue(),
                    toStringList(testDocument.getArray("seeds")),
                    toServerAddressList(testDocument.getArray("hosts")),
                    testDocument.getDocument("options", new BsonDocument())
            });

        }
        return data;
    }

    private static List<String> toStringList(final BsonArray bsonArray) {
        List<String> retVal = new ArrayList<String>(bsonArray.size());
        for (BsonValue cur : bsonArray) {
            retVal.add(cur.asString().getValue());
        }
        return retVal;
    }

    private static List<ServerAddress> toServerAddressList(final BsonArray bsonArray) {
        List<ServerAddress> retVal = new ArrayList<ServerAddress>(bsonArray.size());
        for (BsonValue cur : bsonArray) {
            retVal.add(new ServerAddress(cur.asString().getValue()));
        }
        return retVal;
    }

    private static InitialDirContext createDnsDirContext() {
        Hashtable<String, String> envProps = new Hashtable<String, String>();
        envProps.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
        try {
            return new InitialDirContext(envProps);
        } catch (NamingException e) {
            throw new MongoClientException("Unable to create JNDI context for resolving SRV records", e);
        }
    }
}
