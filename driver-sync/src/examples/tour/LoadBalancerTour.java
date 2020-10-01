/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
 *
 */

package tour;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.TransactionBody;
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
        MongoCollection<Document> collection = client.getDatabase("test").getCollection("transaction");
        collection.drop();

        int count = 100;
        ExecutorService service = Executors.newFixedThreadPool(count);

        CountDownLatch latch = new CountDownLatch(count);
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger failureCounter = new AtomicInteger();
        for (int i = 0; i < count; i++) {
            service.submit(() -> {
                Random random = new Random();
                try (ClientSession clientSession = client.startSession()) {
                    clientSession.withTransaction((TransactionBody<Void>) () -> {
                        for (int j = 0; j < 50; j++) {
                            collection.insertOne(clientSession, new Document());
                            sleepUninterruptibly(random);
                        }
                        //noinspection ConstantConditions
                        return null;
                    });
                } catch (Exception e) {
                    failureCounter.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    System.out.println(counter.incrementAndGet());
                    latch.countDown();
                }
            });
        }
        latch.await(10, TimeUnit.SECONDS);
        service.shutdown();
        if (failureCounter.get() > 0) {
            throw new RuntimeException("At least one transaction failed to complete");
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
                                sleepUninterruptibly(random);
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

        latch.await(10, TimeUnit.SECONDS);
        service.shutdown();
        if (failureCounter.get() > 0) {
            throw new RuntimeException("At least one cursor failed to iterate");
        }
    }

    private static void sleepUninterruptibly(Random random) {
        try {
            Thread.sleep(random.nextInt(5) + 1);
        } catch (InterruptedException e) {
            // nothing
        }
    }
}
