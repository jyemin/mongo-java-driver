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

import com.mongodb.client.model.Mqlv2Source;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Mqlv2FunctionalTest {

    private final MongoDatabase db = Fixture.getDefaultDatabase();

    @Test
    void runsRawStringQuery() {
        List<Document> results = db.mqlv2("from <<{a: 1}, {a: 2}, {a: 3}>> | match a == 2")
                .into(new ArrayList<Document>());
        assertEquals(1, results.size());
        assertEquals(2, results.get(0).getInteger("a"));
    }

    @Test
    void runsAnyExpression() {
        List<Document> results = db.mqlv2(
                "from <<{a: [1, 2, 3]}, {a: [3, 99]}>> | match a* any ($ == 99)")
                .into(new ArrayList<Document>());
        assertEquals(1, results.size());
    }

    @Test
    void runsGroupStage() {
        List<Document> results = db.mqlv2(
                "from <<{a: 1, b: 2}, {a: 1, b: 3}, {a: 2, b: 4}>> | group (k=a) (s=sum($->b))")
                .into(new ArrayList<Document>());
        assertEquals(2, results.size());
    }

    @Test
    void runsUnwindStage() {
        List<Document> results = db.mqlv2(
                "from <<{a: [1, 2, 3]}>> | unwind $i=a* in {a: a, idx: $i}")
                .into(new ArrayList<Document>());
        assertEquals(3, results.size());
    }

    @Test
    void runsTopLevelAgg() {
        Document result = db.mqlv2("from sum(<<1, 2, 3, 4>>)").first();
        assertNotNull(result);
        assertEquals(10, result.getInteger("value"));
    }

    @Test
    void mqlv2SourceOverloadDelegates() {
        Mqlv2Source source = new Mqlv2Source() {
            @Override
            public String toMqlv2() {
                return "from <<1, 2, 3>> | count";
            }
        };
        Document result = db.mqlv2(source).first();
        assertNotNull(result);
        assertEquals(3, result.getInteger("value"));
    }

    @Test
    void rejectsBadQuery() {
        boolean threw = false;
        try {
            db.mqlv2("from not a valid mqlv2 query").into(new ArrayList<Document>());
        } catch (RuntimeException e) {
            threw = true;
            assertTrue(e.getMessage().contains("ERROR")
                    || e.getMessage().contains("mqlv2")
                    || e.getMessage().contains("parse"));
        }
        assertTrue(threw, "Expected an exception for a malformed mqlv2 query");
    }
}
