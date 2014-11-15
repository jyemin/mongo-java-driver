package com.mongodb.operation

import com.mongodb.OperationFunctionalSpecification
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.connection.QueryResult
import org.bson.BsonDocument
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.DocumentCodec

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.getAsyncBinding

class AsyncQueryBatchCursorSpecification extends OperationFunctionalSpecification {
    AsyncConnectionSource connectionSource
    AsyncQueryBatchCursor<Document> cursor

    def setup() {
        for (
                int i = 0;
                i < 10;
                i++) {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', i))
        }
        connectionSource = getAsyncBinding().getReadConnectionSource().get()
    }

    def cleanup() {
        if (cursor != null) {
            cursor.close()
        }
    }

    def 'should exhaust single batch'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(getNamespace(), executeQuery(), 0, 0, new DocumentCodec(), connectionSource)

        expect:
        nextBatch().size() == 10
        !nextBatch()
        connectionSource.count == 1
    }

    def 'should exhaust multiple batches'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(getNamespace(), executeQuery(3), 0, 2, new DocumentCodec(), connectionSource)

        expect:
        nextBatch().size() == 3
        nextBatch().size() == 2
        nextBatch().size() == 2
        nextBatch().size() == 2
        nextBatch().size() == 1
        !nextBatch()
        connectionSource.count == 1
    }

    def 'should respect batch size'() {
        when:
        cursor = new AsyncQueryBatchCursor<Document>(getNamespace(), executeQuery(3), 0, 2, new DocumentCodec(), connectionSource)

        then:
        cursor.batchSize == 2

        when:
        nextBatch()
        cursor.batchSize = 4

        then:
        nextBatch().size() == 4
    }

    def 'should close when exhausted'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(getNamespace(), executeQuery(), 0, 2, new DocumentCodec(), connectionSource)

        when:
        cursor.close()

        then:
        connectionSource.count == 1

        when:
        cursor.next({})

        then:
        thrown(IllegalStateException)
    }

    def 'should close when not exhausted'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(getNamespace(), executeQuery(3), 0, 2, new DocumentCodec(), connectionSource)

        when:
        cursor.close()

        then:
        sleep(500) // racy test, but have to wait for the kill cursor to complete asynchronously
        connectionSource.count == 1
    }

    def 'should block waiting for next batch on a tailable cursor'() {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1).append('ts', new BsonTimestamp(5, 0)))
        def firstBatch = executeQueryProtocol(new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(5, 0))), 2, true, false);


        when:
        cursor = new AsyncQueryBatchCursor<Document>(getNamespace(), firstBatch, 0, 2, new DocumentCodec(), connectionSource)
        def batch = nextBatch()

        then:
        batch.size() == 1
        batch[0].get('_id') == 1

        when:
        Thread.start {
            sleep(500)
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 2).append('ts', new BsonTimestamp(6, 0)))
        }

        batch = nextBatch()

        then:
        batch.size() == 1
        batch[0].get('_id') == 2
    }

    def 'should respect limit'() {
        given:
        cursor = new AsyncQueryBatchCursor<Document>(getNamespace(), executeQuery(3), 6, 2, new DocumentCodec(), connectionSource)

        expect:
        nextBatch().size() == 3
        nextBatch().size() == 2
        nextBatch().size() == 1
        !nextBatch()
    }

    List<Document> nextBatch() {
        def latch = new CountDownLatch(1)
        def nextBatch
        def exception
        cursor.next({ r, e -> nextBatch = r; exception = e; latch.countDown() })
        latch.await(1, TimeUnit.SECONDS)
        if (exception != null) {
            throw exception
        }
        nextBatch
    }

    private QueryResult<Document> executeQuery() {
        executeQuery(0)
    }

    private QueryResult<Document> executeQuery(int numToReturn) {
        executeQueryProtocol(new BsonDocument(), numToReturn, false, false)
    }

    private QueryResult<Document> executeQueryProtocol(BsonDocument query, int numberToReturn, boolean tailable, boolean awaitData) {
        def connection = connectionSource.getConnection().get()
        try {
            connection.queryAsync(getNamespace(), query, null, numberToReturn, 0,
                                  false, tailable, awaitData, false, false, false,
                                  new DocumentCodec()).get()
        } finally {
            connection.release();
        }
    }
}