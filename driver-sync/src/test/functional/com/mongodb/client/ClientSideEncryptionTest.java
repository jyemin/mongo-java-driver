/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client;

import com.mongodb.AutoEncryptOptions;
import com.mongodb.Block;
import com.mongodb.ClientSideEncryptionOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.event.CommandEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.client.CommandMonitoringTestHelper.assertEventsEquality;
import static com.mongodb.client.CommandMonitoringTestHelper.getExpectedEvents;
import static com.mongodb.client.Fixture.getMongoClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests
@RunWith(Parameterized.class)
public class ClientSideEncryptionTest {

    private final String filename;
    private final BsonDocument encryptionSetupDocument;
    private final String description;
    private final BsonArray data;
    private final BsonDocument definition;
    private JsonPoweredCrudTestHelper helper;
    private TestCommandListener commandListener;
    private MongoClient mongoClient;

    public ClientSideEncryptionTest(final String filename,
                                    final BsonDocument encryptionSetupDocument,
                                    final String description, final BsonArray data, final BsonDocument definition) {

        this.filename = filename;
        this.encryptionSetupDocument = encryptionSetupDocument;
        this.description = description;
        this.data = data;
        this.definition = definition;
    }

    @Before
    public void setUp() {
        assumeTrue(canRunTests());

        for (BsonValue cur : encryptionSetupDocument.getArray("collections")) {
            BsonDocument collectionDocument = cur.asDocument();
            String databaseName = collectionDocument.getString("database").getValue();
            String collectionName = collectionDocument.getString("collection").getValue();

            MongoDatabase database = getMongoClient().getDatabase(databaseName);
            MongoCollection<BsonDocument> collection = database
                    .getCollection(collectionName, BsonDocument.class);
            collection.drop();

            if (collectionDocument.containsKey("jsonSchema")) {
                database.createCollection(collectionName, new CreateCollectionOptions()
                        .validationOptions(new ValidationOptions()
                                .validationAction(ValidationAction.WARN)
                                .validator(new BsonDocument("$jsonSchema",
                                        collectionDocument.getDocument("jsonSchema")))));
            }

            if (collectionDocument.containsKey("data")) {
                BsonArray data = collectionDocument.getArray("data", new BsonArray());

                if (!data.isEmpty()) {
                    List<BsonDocument> documents = new ArrayList<BsonDocument>();
                    for (BsonValue document : data) {
                        documents.add(document.asDocument());
                    }
                    collection.insertMany(documents);
                }
            }
        }

        commandListener = new TestCommandListener();

        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applyConnectionString(getConnectionString());
        if (System.getProperty("java.version").startsWith("1.6.")) {
            builder.applyToSslSettings(new Block<SslSettings.Builder>() {
                @Override
                public void apply(final SslSettings.Builder builder) {
                    builder.invalidHostNameAllowed(true);
                }
            });
        }

        BsonDocument clientOptions = definition.getDocument("clientOptions");
        BsonDocument cryptOptions = clientOptions.getDocument("clientSideEncryptionOptions");
        BsonDocument kmsProviders = cryptOptions.getDocument("kmsProviders");
        BsonDocument autoEncryptMapDocument = cryptOptions.getDocument("autoEncryptMap");

        Map<String, AutoEncryptOptions> namespaceToSchemaMap = new HashMap<String, AutoEncryptOptions>();

        for (Map.Entry<String, BsonValue> entries : autoEncryptMapDocument.entrySet()) {
            final BsonDocument autoEncryptOptionsDocument = entries.getValue().asDocument();
            namespaceToSchemaMap.put(entries.getKey(),
                    new AutoEncryptOptions(autoEncryptMapDocument.getBoolean("enabled", BsonBoolean.TRUE).getValue(),
                            autoEncryptOptionsDocument.getDocument("schema", null)));
        }

        Map<String, Map<String, Object>> kmsProvidersMap = new HashMap<String, Map<String, Object>>();

        for (String kmsProviderKey : kmsProviders.keySet()) {
            BsonDocument kmsProviderOptions = kmsProviders.get(kmsProviderKey).asDocument();
            Map<String, Object> kmsProviderMap = new HashMap<String, Object>();

            if (kmsProviderKey.equals("aws")) {
                kmsProviderMap.put("accessKeyId", System.getProperty("org.mongodb.test.awsAccessKeyId"));
                kmsProviderMap.put("secretAccessKey", System.getProperty("org.mongodb.test.awsSecretAccessKey"));
                kmsProvidersMap.put("aws", kmsProviderMap);
            } else if (kmsProviderKey.equals("local")) {
                kmsProviderMap.put("key", kmsProviderOptions.getBinary("key").getData());
                kmsProvidersMap.put("local", kmsProviderMap);
            }
        }

        mongoClient = MongoClients.create(builder
                .clientSideEncryptionOptions(new ClientSideEncryptionOptions(null, "admin.datakeys",
                        kmsProvidersMap, namespaceToSchemaMap, Collections.<String, Object>emptyMap()))
                .addCommandListener(commandListener)
                .applyToSocketSettings(new Block<SocketSettings.Builder>() {
                    @Override
                    public void apply(final SocketSettings.Builder builder) {
                        builder.readTimeout(5, TimeUnit.SECONDS);
                    }
                })
                .build());

        MongoDatabase database = mongoClient.getDatabase("default");
        helper = new JsonPoweredCrudTestHelper(description, database, database.getCollection("default", BsonDocument.class));
    }

    @After
    public void cleanUp() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        for (BsonValue cur : definition.getArray("operations")) {
            BsonDocument operation = cur.asDocument();
            BsonValue expectedResult = operation.get("result");
            BsonDocument actualOutcome = helper.getOperationResults(operation);
            if (expectedResult != null) {
                BsonValue actualResult = actualOutcome.get("result");
                assertEquals("Expected operation result differs from actual", expectedResult, actualResult);
            }
        }

        if (definition.containsKey("expectations")) {
            List<CommandEvent> expectedEvents = getExpectedEvents(definition.getArray("expectations"), "default", null);
            List<CommandEvent> events = commandListener.getCommandStartedEvents();

//            for (CommandEvent event : events) {
//                System.out.println(((CommandStartedEvent) event).getCommand().toJson(JsonWriterSettings.builder().indent(true).build()));
//                System.out.println();
//            }
//
            // TODO: these don't match
            assertEventsEquality(expectedEvents, events);
        }

    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/client-side-encryption")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            BsonDocument encryptionSetupDocument = testDocument.getDocument("encryption_setup");
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), encryptionSetupDocument, test.asDocument().getString("description").getValue(),
                        testDocument.getArray("data", new BsonArray()), test.asDocument()});
            }
        }
        return data;
    }

    private boolean canRunTests() {
        return true;
    }

}
