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

package com.mongodb.client.unified;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

final class WireMessageMatcher {
    private final ValueMatcher valueMatcher;
    private final AssertionContext context;

    WireMessageMatcher(final ValueMatcher valueMatcher, final AssertionContext context) {
        this.valueMatcher = valueMatcher;
        this.context = context;
    }

    /**
     * {@code command} is shorthand: in an OP_MSG, or in the message of an OP_COMPRESSED, it stands for a single kind 0 section,
     * and in an OP_QUERY it stands for the query.
     */
    static BsonDocument expand(final BsonDocument expected) {
        BsonDocument result = expected.clone();
        if (result.containsKey("message") && result.get("message").isDocument()) {
            result.put("message", expand(result.getDocument("message")));
        }
        if (result.containsKey("command")) {
            BsonValue command = result.remove("command");
            boolean isQuery = "OP_QUERY".equals(result.containsKey("opcode") ? result.getString("opcode").getValue() : null)
                    || result.containsKey("query") || result.containsKey("fullCollectionName");
            if (isQuery) {
                result.put("query", command);
            } else {
                result.put("sections", new BsonArray(singletonList(
                        new BsonDocument("kind", new BsonInt32(0)).append("body", command))));
            }
        }
        return result;
    }

    /**
     * The sections of an OP_MSG may be in any order, so the sections of the actual message are put in the order of the expected
     * ones before matching. An expected section is paired with the first unused actual section of the same kind, and for kind 1
     * of the same identifier. Actual sections that no expected section is paired with go last, so that they fail the match.
     * The documents within a kind 1 section stay in order. This also applies to the message of an OP_COMPRESSED.
     */
    static BsonDocument alignSections(final BsonDocument expected, final BsonDocument actual) {
        BsonDocument result = actual.clone();
        if (expected.containsKey("message") && expected.get("message").isDocument()
                && actual.containsKey("message") && actual.get("message").isDocument()) {
            result.put("message", alignSections(expected.getDocument("message"), actual.getDocument("message")));
        }
        if (expected.containsKey("sections") && expected.get("sections").isArray()
                && actual.containsKey("sections") && actual.get("sections").isArray()) {
            List<BsonValue> unused = new ArrayList<>(actual.getArray("sections").getValues());
            BsonArray ordered = new BsonArray();
            for (BsonValue expectedSection : expected.getArray("sections")) {
                BsonValue paired = null;
                if (expectedSection.isDocument()) {
                    for (BsonValue candidate : unused) {
                        if (candidate.isDocument() && sameSection(expectedSection.asDocument(), candidate.asDocument())) {
                            paired = candidate;
                            break;
                        }
                    }
                }
                if (paired != null) {
                    unused.remove(paired);
                    ordered.add(paired);
                }
            }
            ordered.addAll(unused);
            result.put("sections", ordered);
        }
        return result;
    }

    private static boolean sameSection(final BsonDocument expected, final BsonDocument actual) {
        if (!expected.containsKey("kind") || !expected.get("kind").equals(actual.get("kind"))) {
            return false;
        }
        return !expected.get("kind").equals(new BsonInt32(1))
                || !expected.containsKey("identifier") || expected.get("identifier").equals(actual.get("identifier"));
    }

    void assertWireMessageEquality(final String client, final boolean ignoreExtraMessages, final BsonArray expectedMessages,
            final List<WireMessage> actualMessages) {
        List<BsonDocument> actualDocuments = actualMessages.stream().map(WireMessage::toDocument).collect(Collectors.toList());
        context.push(ContextElement.ofWireMessages(client, expectedMessages, actualDocuments));

        if (expectedMessages.isEmpty()) {
            // An empty list asserts that nothing was written, even when ignoreExtraMessages is true.
            assertEquals(context.getMessage("Expected no wire messages"), 0, actualDocuments.size());
        } else if (ignoreExtraMessages) {
            assertTrue(context.getMessage("Number of wire messages must be greater than or equal to the expected number of messages"),
                    actualDocuments.size() >= expectedMessages.size());
        } else {
            assertEquals(context.getMessage("Number of wire messages must be the same"), expectedMessages.size(),
                    actualDocuments.size());
        }

        for (int i = 0; i < expectedMessages.size(); i++) {
            // Strict at every level: a field or key that was written but is not expected is a failure.
            BsonDocument expected = expand(expectedMessages.get(i).asDocument());
            valueMatcher.assertValuesMatchStrict(expected, alignSections(expected, actualDocuments.get(i)));
        }

        context.pop();
    }
}
