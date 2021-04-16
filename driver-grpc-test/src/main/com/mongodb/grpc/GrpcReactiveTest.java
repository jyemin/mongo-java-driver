package com.mongodb.grpc;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bson.Document;
import reactor.core.publisher.Mono;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class GrpcReactiveTest {
    public static void main(String[] args) throws InterruptedException, ParseException {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("numOperations")
                .desc("Total number of operations executed")
                .hasArg()
                .type(Integer.class)
                .required(false)
                .build());
        options.addOption(Option.builder().longOpt("numConcurrentOperations")
                .desc("Number of concurrent operations")
                .hasArg()
                .type(Integer.class)
                .required(false)
                .build());
        options.addOption(Option.builder().longOpt("gRpcHostPort")
                .desc("Host and port to use for gRPC-based implementation")
                .hasArg()
                .type(String.class)
                .required(false)
                .build());
        options.addOption(Option.builder().longOpt("mongoHostPort")
                .desc("Host and port to use for regular implementation")
                .hasArg()
                .type(String.class)
                .required(false)
                .build());
        options.addOption(Option.builder().longOpt("warmup")
                .desc("Whether to include a warmup phase")
                .hasArg(false)
                .type(Boolean.class)
                .required(false)
                .build());
        options.addOption(Option.builder().longOpt("warmupDivisor")
                .desc("Number to divide numberOperations by for warmup phase")
                .hasArg()
                .type(Integer.class)
                .required(false)
                .build());
        
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String mongoConnectionString = null;
        if (cmd.hasOption("mongoHostPort")) {
            mongoConnectionString = "mongodb://" + cmd.getOptionValue("mongoHostPort");
        }
        String grpcConnectionString = null;
        if (cmd.hasOption("gRpcHostPort")) {
            grpcConnectionString = "mongodb://" + cmd.getOptionValue("gRpcHostPort") + "/?grpc=true";
        }

        int numOperations = 10_0000;
        if (cmd.hasOption("numOperations")) {
            numOperations= Integer.parseInt(cmd.getOptionValue("numOperations"));
        }
        int concurrentOperations = 100;
        if (cmd.hasOption("numConcurrentOperations")) {
            concurrentOperations = Integer.parseInt(cmd.getOptionValue("numConcurrentOperations"));
        }

        boolean warmup = cmd.hasOption("warmup");
        int warmupDivisor = 10;
        if (cmd.hasOption("warmupDivisor")) {
            warmupDivisor = Integer.parseInt(cmd.getOptionValue("warmupDivisor"));
        }

        if (grpcConnectionString == null && mongoConnectionString == null) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("gRPC Load Tester", options);
            System.exit(1);
        }

        // warmup
        if (warmup) {
            System.out.println("Warming up with " + numOperations / warmupDivisor + " operations");
            if (mongoConnectionString != null) {
                runBenchmark(mongoConnectionString, numOperations / warmupDivisor, concurrentOperations);
            }
            if (grpcConnectionString != null) {
                runBenchmark(grpcConnectionString, numOperations / warmupDivisor, concurrentOperations);
            }
            System.out.println("Warmed up");
            System.out.println();
        }

        long operationsPerSecond = 0;
        if (mongoConnectionString != null) {
            System.out.println("Running benchmark on legacy protocol");
            operationsPerSecond = runBenchmark(mongoConnectionString, numOperations, concurrentOperations);
        }

        Thread.sleep(5000);

        long grpOperationsPerSecond = 0;
        if (grpcConnectionString != null) {
            System.out.println("Running benchmark on gRPC protocol");
            grpOperationsPerSecond = runBenchmark(grpcConnectionString, numOperations, concurrentOperations);
        }

        System.out.println();
        System.out.printf("Concurrent Operations: %,d%n", concurrentOperations);
        System.out.printf("Total Operations: %,d%n", numOperations);
        if (mongoConnectionString != null) {
            System.out.printf("Mongo: %d ops/sec%n", operationsPerSecond);
        }
        if (grpcConnectionString != null) {
            System.out.printf("gRPC:   %d ops/sec%n", grpOperationsPerSecond);
        }
    }

    private static long runBenchmark(final String connectionString, final int numOperations, final int concurrentOperations)
            throws InterruptedException {
        try (MongoClient client = MongoClients.create(
                MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(connectionString))
                        .applyToConnectionPoolSettings(builder -> builder.maxSize(Integer.MAX_VALUE))
                        .build())) {

            Thread.sleep(500);
            MongoDatabase database = client.getDatabase("test");
            MongoCollection<Document> collection = database.getCollection("test");

            Mono.from(collection.drop()).block();
            Mono.from(collection.insertOne(new Document())).block();

            CountDownLatch latch = new CountDownLatch(numOperations);
            Semaphore semaphore = new Semaphore(concurrentOperations);

            long startTime = System.nanoTime();

            for (int i = 0; i < numOperations; i++) {
                semaphore.acquire();
                Mono.from(collection.find().first()).subscribe(
                        document -> {
                            semaphore.release();
                            latch.countDown();
                        },
                        t -> {
                            t.printStackTrace();
                            semaphore.release();
                            latch.countDown();
                        });
            }

            if (!latch.await(1, TimeUnit.MINUTES)) {
                System.out.println("Something is wrong");
                System.exit(1);
            }

            double durationInSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
            return Math.round(numOperations / durationInSeconds);
        }
    }
}
