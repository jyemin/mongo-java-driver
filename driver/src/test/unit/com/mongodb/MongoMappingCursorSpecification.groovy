package com.mongodb

import com.mongodb.client.MongoCursor
import org.bson.Document
import spock.lang.Specification

class MongoMappingCursorSpecification extends Specification {
    def 'should get server cursor and address'() {
        given:
        def cursor = Stub(MongoCursor)
        def address = new ServerAddress('host', 27018)
        def serverCursor = new ServerCursor(5, address)
        cursor.getServerAddress() >> address
        cursor.getServerCursor() >> serverCursor
        def mappingCursor = new MongoMappingCursor(cursor, { })

        expect:
        mappingCursor.serverAddress.is(address)
        mappingCursor.serverCursor.is(serverCursor)
    }

    def 'should throw on remove'() {
        given:
        def cursor = Stub(MongoCursor)
        cursor.remove() >> { throw new UnsupportedOperationException() }
        def mappingCursor = new MongoMappingCursor(cursor, { })

        when:
        mappingCursor.remove()

        then:
        thrown(UnsupportedOperationException)
    }

    def 'should close cursor'() {
        given:
        def cursor = Mock(MongoCursor)
        def mappingCursor = new MongoMappingCursor(cursor, { })

        when:
        mappingCursor.close()

        then:
        1 * cursor.close()
    }

    def 'should have next if cursor does'() {
        given:
        def cursor = Stub(MongoCursor)
        cursor.hasNext() >>> [true, false]
        def mappingCursor = new MongoMappingCursor(cursor, { })

        expect:
        mappingCursor.hasNext()
        !mappingCursor.hasNext()
    }

    def 'should map next'() {
        given:
        def cursor = Stub(MongoCursor)
        cursor.next() >> new Document('_id', 1)
        def mappingCursor = new MongoMappingCursor(cursor, { Document d -> d.get('_id') })

        expect:
        mappingCursor.next() == 1
    }

    def 'should map try next'() {
        given:
        def cursor = Stub(MongoCursor)
        cursor.tryNext() >>> [new Document('_id', 1), null]
        def mappingCursor = new MongoMappingCursor(cursor, { Document d -> d.get('_id') })

        expect:
        mappingCursor.tryNext() == 1
        !mappingCursor.tryNext()
    }
}