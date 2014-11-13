package com.mongodb

import com.mongodb.operation.BatchCursor
import org.bson.Document
import spock.lang.Specification


class MongoBatchCursorAdapterSpecification extends Specification {
    def 'should get server cursor and address'() {
        given:
        def batchCursor = Stub(BatchCursor)
        def address = new ServerAddress('host', 27018)
        def serverCursor = new ServerCursor(5, address)
        batchCursor.getServerAddress() >> address
        batchCursor.getServerCursor() >> serverCursor
        def cursor = new MongoBatchCursorAdapter(batchCursor)

        expect:
        cursor.serverAddress.is(address)
        cursor.serverCursor.is(serverCursor)
    }

    def 'should throw on remove'() {
        given:
        def batchCursor = Stub(BatchCursor)
        def cursor = new MongoBatchCursorAdapter(batchCursor)

        when:
        cursor.remove()

        then:
        thrown(UnsupportedOperationException)
    }

    def 'should close batch cursor'() {
        given:
        def batchCursor = Mock(BatchCursor)
        def cursor = new MongoBatchCursorAdapter(batchCursor)

        when:
        cursor.close()

        then:
        1 * batchCursor.close()
    }

    def 'next should throw if there is no next'() {
        given:
        def batchCursor = Stub(BatchCursor)

        batchCursor.hasNext() >> false

        def cursor = new MongoBatchCursorAdapter(batchCursor)

        when:
        cursor.next()

        then:
        thrown(NoSuchElementException)
    }


    def 'should get next from batch cursor'() {
        given:
        def firstBatch = [new Document('x', 1), new Document('x', 1)]
        def secondBatch = [new Document('x', 2)]

        def batchCursor = Stub(BatchCursor)

        batchCursor.hasNext() >>> [true, true, true, true, false]
        batchCursor.next() >>> [firstBatch, secondBatch]

        def cursor = new MongoBatchCursorAdapter(batchCursor)

        expect:
        cursor.hasNext()
        cursor.next() == firstBatch[0]
        cursor.hasNext()
        cursor.next() == firstBatch[1]
        cursor.hasNext()
        cursor.next() == secondBatch[0]
        !cursor.hasNext()
    }

    def 'should try next from batch cursor'() {
        given:
        def firstBatch = [new Document('x', 1), new Document('x', 1)]
        def secondBatch = [new Document('x', 2)]

        def batchCursor = Stub(BatchCursor)

        batchCursor.tryNext() >>> [firstBatch, null, secondBatch, null]

        def cursor = new MongoBatchCursorAdapter(batchCursor)

        expect:
        cursor.tryNext() == firstBatch[0]
        cursor.tryNext() == firstBatch[1]
        cursor.tryNext() == null
        cursor.tryNext() == secondBatch[0]
        cursor.tryNext() == null
    }
}