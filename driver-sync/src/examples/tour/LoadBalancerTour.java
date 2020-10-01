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

package tour;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadBalancerTour {
    public static void main(String[] args) throws InterruptedException {
        ConnectionString uri = new ConnectionString("mongodb://localhost:37017");
        MongoClient client = MongoClients.create(
                MongoClientSettings.builder()
                        .applyConnectionString(uri)
                        .applyToConnectionPoolSettings(builder ->
                                builder.maxConnectionLifeTime(10, TimeUnit.SECONDS))
                        .build());

        testCursor(client);
        testTransaction(client);
    }


    private static void testTransaction(final MongoClient client) throws InterruptedException {

        int count = 10;

        for (int i = 0; i < count; i++) {
            MongoCollection<Document> collection = client.getDatabase("test").getCollection("transaction-" + i);
            collection.drop();
        }

        ExecutorService service = Executors.newFixedThreadPool(count);

        CountDownLatch latch = new CountDownLatch(count);
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger failureCounter = new AtomicInteger();
        for (int i = 0; i < count; i++) {
            MongoCollection<Document> collection = client.getDatabase("test").getCollection("transaction-" + i);
            service.submit(() -> {
                Random random = new Random();
                try (ClientSession clientSession = client.startSession()) {
                    try {
                        clientSession.startTransaction();
                        for (int j = 0; j < 10; j++) {
                            collection.insertOne(clientSession, new Document());
                            sleepUninterruptedly(random);
                        }
                        clientSession.commitTransaction();
                    } catch (Exception e) {
                        if (clientSession.hasActiveTransaction()) {
                            clientSession.abortTransaction();
                        }
                        throw e;
                    }
                } catch (Exception e) {
                    failureCounter.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    System.out.println(counter.incrementAndGet());
                    latch.countDown();
                }
            });
        }
        latch.await(5, TimeUnit.MINUTES);
        service.shutdown();
        if (failureCounter.get() > 0) {
            throw new RuntimeException(failureCounter.get() + " failures");
        }
    }

    private static void testCursor(final MongoClient client) throws InterruptedException {
        MongoCollection<Document> collection = client.getDatabase("test").getCollection("cursor");
        collection.drop();
        for (int i = 0; i < 50; i++) {
            collection.insertOne(new Document());
        }

        int count = 100;
        ExecutorService service = Executors.newFixedThreadPool(count);

        CountDownLatch latch = new CountDownLatch(count);
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger failureCounter = new AtomicInteger();
        for (int i = 0; i < count; i++) {
            service.submit(() -> {
                        Random random = new Random();
                        try (MongoCursor<Document> cursor = collection.find().batchSize(1).cursor()) {
                            while (cursor.hasNext()) {
                                cursor.next();
                                sleepUninterruptedly(random);
                            }
                        } catch (Exception e) {
                            failureCounter.incrementAndGet();
                            e.printStackTrace();
                        } finally {
                            System.out.println(counter.incrementAndGet());
                            latch.countDown();
                        }
                    }
            );
        }

        latch.await(1, TimeUnit.MINUTES);
        service.shutdown();
        if (failureCounter.get() > 0) {
            throw new RuntimeException(failureCounter.get() + " failures");
        }
    }

    private static void sleepUninterruptedly(Random random) {
        try {
            Thread.sleep(random.nextInt(5) + 1);
        } catch (InterruptedException e) {
            // nothing
        }
    }
}
