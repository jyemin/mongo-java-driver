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

package org.bson.codecs.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.jackson.samples.AllTypesPojo;
import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class JacksonCodecTest {
    @Test
    public void testRoundTrip() {
        AllTypesPojo pojo = new AllTypesPojo();
        pojo.setB(true);
        pojo.setI(5);
        pojo.setL(6L);
//        pojo.setDate(new Date(42));
//        pojo.setId(new ObjectId());
//        pojo.setDec(Decimal128.parse("42.0"));
        pojo.setListOfInt(asList(1, 2, 3));

        Codec<AllTypesPojo> codec = new JacksonCodec<>(AllTypesPojo.class, new ObjectMapper(JacksonCodecProvider.getBsonFactory()));
        BsonDocument encodedBsonDocument = new BsonDocument();
        codec.encode(new BsonDocumentWriter(encodedBsonDocument), pojo, EncoderContext.builder().build());

        assertEquals(new BsonDocument()
                        .append("id", BsonNull.VALUE)
                        .append("i", new BsonInt32(5))
                        .append("l", new BsonInt64(6))
                        .append("b", BsonBoolean.TRUE)
                        .append("date", BsonNull.VALUE)
                        .append("dec", BsonNull.VALUE)
                        .append("listOfInt",
                                new BsonArray(asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)))),
                encodedBsonDocument);

        AllTypesPojo decodedPojo = codec.decode(new BsonDocumentReader(encodedBsonDocument), DecoderContext.builder().build());

        assertEquals(pojo, decodedPojo);
    }
}
