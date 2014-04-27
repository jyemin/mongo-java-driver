package org.mongodb.async;

import org.mongodb.MongoClientOptions;
import org.mongodb.MongoClientURI;
import org.mongodb.MongoCredential;
import org.mongodb.connection.AsynchronousSocketChannelStreamFactory;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.ClusterConnectionMode;
import org.mongodb.connection.ClusterSettings;
import org.mongodb.connection.DefaultClusterFactory;
import org.mongodb.connection.SSLSettings;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SocketSettings;
import org.mongodb.connection.StreamFactory;
import org.mongodb.connection.netty.NettyStreamFactory;
import org.mongodb.management.JMXConnectionPoolListener;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A factory for MongoClient instances.
 *
 * @since 3.0
 */
public final class MongoClients {
    /**
     * Create a new client with the given URI and options.
     *
     * @param mongoURI the URI of the cluster to connect to
     * @param options the options, which override the options from the URI
     * @return the client
     * @throws UnknownHostException
     */
    public static MongoClient create(final MongoClientURI mongoURI, final MongoClientOptions options)
    throws UnknownHostException {
        if (mongoURI.getHosts().size() == 1) {
            return new MongoClientImpl(options, createCluster(ClusterSettings.builder()
                                                                             .mode(ClusterConnectionMode.SINGLE)
                                                                             .hosts(Arrays.asList(new ServerAddress(mongoURI.getHosts()
                                                                                                                            .get(0))))
                                                                             .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                                                             .build(),
                                                              mongoURI.getCredentialList(), options, getStreamFactory(options)
                                                             ));
        } else {
            List<ServerAddress> seedList = new ArrayList<ServerAddress>();
            for (final String cur : mongoURI.getHosts()) {
                seedList.add(new ServerAddress(cur));
            }
            return new MongoClientImpl(options, createCluster(ClusterSettings.builder()
                                                                             .hosts(seedList)
                                                                             .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                                                             .build(),
                                                              mongoURI.getCredentialList(), options, getStreamFactory(options)
                                                             ));
        }
    }

    private static Cluster createCluster(final ClusterSettings clusterSettings, final List<MongoCredential> credentialList,
                                         final MongoClientOptions options, final StreamFactory streamFactory) {
        StreamFactory heartbeatStreamFactory = getHeartbeatStreamFactory(options);
        return new DefaultClusterFactory().create(clusterSettings, options.getServerSettings(),
                                                  options.getConnectionPoolSettings(), streamFactory,
                                                  heartbeatStreamFactory,
                                                  credentialList, null, new JMXConnectionPoolListener(), null);
    }

    private static StreamFactory getHeartbeatStreamFactory(final MongoClientOptions options) {
        return getStreamFactory(options.getHeartbeatSocketSettings(), options.getSslSettings());
    }

    private static StreamFactory getStreamFactory(final MongoClientOptions options) {
        return getStreamFactory(options.getSocketSettings(), options.getSslSettings());
    }

    private static StreamFactory getStreamFactory(final SocketSettings socketSettings,
                                                  final SSLSettings sslSettings) {
        String streamType = System.getProperty("org.mongodb.async.type", "nio2");

        if (streamType.equals("netty")) {
            return new NettyStreamFactory(socketSettings, sslSettings);
        } else if (streamType.equals("nio2")) {
            if (sslSettings.isEnabled()) {
                throw new IllegalArgumentException("Unsupported stream type " + streamType + " when SSL is enabled. Please use Netty " +
                                                   "instead");
            }
            return new AsynchronousSocketChannelStreamFactory(socketSettings, sslSettings);
        } else {
            throw new IllegalArgumentException("Unsupported stream type " + streamType);
        }
    }

    private MongoClients() {
    }
}
