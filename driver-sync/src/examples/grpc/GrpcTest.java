package grpc;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GrpcTest {
    public static void main(String[] args) throws InterruptedException {
        String connectionString = "mongodb://localhost:27017";
        String grpcConnectionString = "mongodb://localhost:50052/?grpc=true";

        // Ensure that numOperations is a multiple of concurrentOperations
        int numOperations = 1_000;
        int concurrentOperations = 1;

        // warmup
        runBenchmark(connectionString, numOperations / 10, concurrentOperations);
        runBenchmark(grpcConnectionString, numOperations / 10, concurrentOperations);
        System.out.println("Warmed up");
        System.out.println();

        System.out.println("Running benchmark on legacy protocol");
        long operationsPerSecond = runBenchmark(connectionString, numOperations, concurrentOperations);
        System.out.println("Running benchmark on gRPC protocol");
        long grpOperationsPerSecond = runBenchmark(grpcConnectionString, numOperations, concurrentOperations);

        System.out.println();
        System.out.printf("Concurrent Operations: %,d%n", concurrentOperations);
        System.out.printf("Total Operations: %,d%n", numOperations);
        System.out.printf("Legacy: %d ops/sec%n", operationsPerSecond);
        System.out.printf("gRPC:   %d ops/sec%n", grpOperationsPerSecond);
    }

    private static long runBenchmark(final String connectionString, final int numOperations, final int concurrentOperations)
            throws InterruptedException {
        try (MongoClient client = MongoClients.create(
                MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(connectionString))
                        .applyToConnectionPoolSettings(builder -> builder.maxSize(Integer.MAX_VALUE))
                        .build())) {

            MongoDatabase database = client.getDatabase("test");
            MongoCollection<Document> collection = database.getCollection("test");

            collection.drop();
            collection.insertOne(new Document());

            long startTime = System.nanoTime();

            ExecutorService service = Executors.newFixedThreadPool(concurrentOperations);

            for (int i = 0; i < concurrentOperations; i++) {
                service.submit(() -> {
                    for (int j = 0; j < numOperations / concurrentOperations; j++) {
                        database.runCommand(new Document("ping", 1));
//                            collection.find().first();
                    }
                });
            }

            service.shutdown();
            if (!service.awaitTermination(5, TimeUnit.MINUTES)) {
                System.out.println("Something is wrong");
                System.exit(1);
            }

            double durationInSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
            return Math.round(numOperations / durationInSeconds);
        }
    }
}

