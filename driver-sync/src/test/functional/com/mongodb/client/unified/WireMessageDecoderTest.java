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

import com.mongodb.ServerAddress;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the POC wire message decoder and matcher with hand built messages, because a real server refuses most malformed ones.
 */
final class WireMessageDecoderTest {
    private static final int OP_MSG = 2013;
    private static final int OP_COMPRESSED = 2012;

    @Test
    @DisplayName("decodes a plain OP_MSG into flags and one kind 0 section")
    void decodesPlainOpMsg() {
        BsonDocument decoded = WireMessageDecoder.decode(opMsg(0, section0("{ping: 1}")));
        assertEquals(BsonDocument.parse("{opcode: 'OP_MSG', responseTo: 0, flags: [], sections: [{kind: 0, body: {ping: 1}}]}"), decoded);
    }

    @Test
    @DisplayName("an extra kind 1 section is visible, and a message that omits it does not match")
    void extraSectionIsVisibleAndFailsStrictMatch() {
        byte[] bytes = opMsg(0, concat(section0("{ping: 1}"), section1("telemetry")));
        BsonDocument decoded = WireMessageDecoder.decode(bytes);
        assertEquals(2, decoded.getArray("sections").size());

        BsonDocument expected = WireMessageMatcher.expand(BsonDocument.parse("{opcode: 'OP_MSG', responseTo: 0, flags: [], command: {ping: 1}}"));
        AssertionError error = assertThrows(AssertionError.class, () -> matcher().assertValuesMatchStrict(expected, decoded));
        assertTrue(error.getMessage().contains("Arrays must be the same size"), error.getMessage());
    }

    @Test
    @DisplayName("a kind 3 telemetry section is decoded like a document, and a message that omits it does not match")
    void telemetrySectionIsDecodedAndFailsStrictMatch() {
        String traceContext = "{otel: {traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'}}";
        BsonDocument decoded = WireMessageDecoder.decode(opMsg(0, concat(section0("{ping: 1}"), section3(traceContext))));
        assertEquals(BsonDocument.parse("{opcode: 'OP_MSG', responseTo: 0, flags: [], sections: ["
                + "{kind: 0, body: {ping: 1}}, {kind: 3, body: " + traceContext + "}]}"), decoded);

        BsonDocument withoutTelemetry = WireMessageMatcher.expand(BsonDocument.parse(
                "{opcode: 'OP_MSG', responseTo: 0, flags: [], command: {ping: 1}}"));
        AssertionError error = assertThrows(AssertionError.class,
                () -> matcher().assertValuesMatchStrict(withoutTelemetry, WireMessageMatcher.alignSections(withoutTelemetry, decoded)));
        assertTrue(error.getMessage().contains("Arrays must be the same size"), error.getMessage());

        BsonDocument withTelemetry = BsonDocument.parse("{opcode: 'OP_MSG', responseTo: 0, flags: [], sections: ["
                + "{kind: 0, body: {ping: 1}}, {kind: 3, body: {otel: {traceparent: {$$type: 'string'}}}}]}");
        matcher().assertValuesMatchStrict(withTelemetry, WireMessageMatcher.alignSections(withTelemetry, decoded));
    }

    @Test
    @DisplayName("sections are matched without regard to their order")
    void sectionsMatchInAnyOrder() {
        String traceContext = "{otel: {traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'}}";
        BsonDocument telemetryFirst = WireMessageDecoder.decode(opMsg(0, concat(section3(traceContext), section0("{ping: 1}"))));
        BsonDocument expected = BsonDocument.parse("{opcode: 'OP_MSG', responseTo: 0, flags: [], sections: ["
                + "{kind: 0, body: {ping: 1}}, {kind: 3, body: {otel: {traceparent: {$$type: 'string'}}}}]}");
        matcher().assertValuesMatchStrict(expected, WireMessageMatcher.alignSections(expected, telemetryFirst));

        // a section that is in the message but is not expected, and an expected section that is not in the message, both fail
        BsonDocument expectedWithoutTelemetry = BsonDocument.parse(
                "{opcode: 'OP_MSG', responseTo: 0, flags: [], sections: [{kind: 0, body: {ping: 1}}]}");
        assertThrows(AssertionError.class, () -> matcher().assertValuesMatchStrict(expectedWithoutTelemetry,
                WireMessageMatcher.alignSections(expectedWithoutTelemetry, telemetryFirst)));
        BsonDocument withoutTelemetry = WireMessageDecoder.decode(opMsg(0, section0("{ping: 1}")));
        assertThrows(AssertionError.class, () -> matcher().assertValuesMatchStrict(expected,
                WireMessageMatcher.alignSections(expected, withoutTelemetry)));

        // reordering does not hide a difference in the content of a section
        BsonDocument wrongBody = BsonDocument.parse("{opcode: 'OP_MSG', responseTo: 0, flags: [], sections: ["
                + "{kind: 0, body: {ping: 2}}, {kind: 3, body: {otel: {traceparent: {$$type: 'string'}}}}]}");
        assertThrows(AssertionError.class, () -> matcher().assertValuesMatchStrict(wrongBody,
                WireMessageMatcher.alignSections(wrongBody, telemetryFirst)));
    }

    @Test
    @DisplayName("kind 1 sections are paired by identifier, and the documents within one stay in order")
    void documentSequencesPairByIdentifier() {
        BsonDocument decoded = WireMessageDecoder.decode(opMsg(0, concat(
                section1("nsInfo", "{ns: 'a.b'}"), section0("{bulkWrite: 1}"), section1("ops", "{insert: 0}", "{insert: 1}"))));
        BsonDocument expected = BsonDocument.parse("{opcode: 'OP_MSG', responseTo: 0, flags: [], sections: ["
                + "{kind: 0, body: {bulkWrite: 1}},"
                + "{kind: 1, identifier: 'ops', documents: [{insert: 0}, {insert: 1}]},"
                + "{kind: 1, identifier: 'nsInfo', documents: [{ns: 'a.b'}]}]}");
        matcher().assertValuesMatchStrict(expected, WireMessageMatcher.alignSections(expected, decoded));

        BsonDocument documentsReordered = BsonDocument.parse("{opcode: 'OP_MSG', responseTo: 0, flags: [], sections: ["
                + "{kind: 0, body: {bulkWrite: 1}},"
                + "{kind: 1, identifier: 'ops', documents: [{insert: 1}, {insert: 0}]},"
                + "{kind: 1, identifier: 'nsInfo', documents: [{ns: 'a.b'}]}]}");
        assertThrows(AssertionError.class, () -> matcher().assertValuesMatchStrict(documentsReordered,
                WireMessageMatcher.alignSections(documentsReordered, decoded)));
    }

    @Test
    @DisplayName("sections inside an OP_COMPRESSED are also matched without regard to their order")
    void compressedSectionsMatchInAnyOrder() throws IOException {
        byte[] inner = opMsg(0, concat(section3("{otel: {traceparent: 'x'}}"), section0("{ping: 1}")));
        byte[] innerBody = new byte[inner.length - 16];
        System.arraycopy(inner, 16, innerBody, 0, innerBody.length);
        BsonDocument decoded = WireMessageDecoder.decode(compress(innerBody, innerBody.length));

        BsonDocument expected = WireMessageMatcher.expand(BsonDocument.parse("{opcode: 'OP_COMPRESSED', responseTo: 0,"
                + " originalOpcode: 'OP_MSG', uncompressedSize: " + innerBody.length + ", compressorId: 2,"
                + " message: {flags: [], sections: [{kind: 0, body: {ping: 1}}, {kind: 3, body: {otel: {traceparent: 'x'}}}]}}"));
        matcher().assertValuesMatchStrict(expected, WireMessageMatcher.alignSections(expected, decoded));
    }

    @Test
    @DisplayName("an empty list of expected messages asserts that none were written, even with ignoreExtraMessages")
    void emptyExpectedMessagesAssertsNoneWereWritten() {
        WireMessageMatcher wireMessageMatcher = new WireMessageMatcher(matcher(), new AssertionContext());
        WireMessage written = new WireMessage(WireMessage.Source.MONITOR, WireMessage.Phase.OPERATION, new ServerAddress(),
                opMsg(0, section0("{hello: 1}")));

        wireMessageMatcher.assertWireMessageEquality("client", true, new BsonArray(), emptyList());
        wireMessageMatcher.assertWireMessageEquality("client", false, new BsonArray(), emptyList());
        assertThrows(AssertionError.class,
                () -> wireMessageMatcher.assertWireMessageEquality("client", true, new BsonArray(), singletonList(written)));
        assertThrows(AssertionError.class,
                () -> wireMessageMatcher.assertWireMessageEquality("client", false, new BsonArray(), singletonList(written)));
    }

    @Test
    @DisplayName("an unexpected flag is visible and fails the match")
    void unexpectedFlagFailsMatch() {
        BsonDocument decoded = WireMessageDecoder.decode(opMsg(1 << 16, section0("{ping: 1}")));
        assertEquals(new BsonArray(singletonList(new BsonString("exhaustAllowed"))), decoded.get("flags"));
        BsonDocument expected = WireMessageMatcher.expand(BsonDocument.parse("{opcode: 'OP_MSG', responseTo: 0, flags: [], command: {ping: 1}}"));
        assertThrows(AssertionError.class, () -> matcher().assertValuesMatchStrict(expected, decoded));
    }

    @Test
    @DisplayName("an extra key in the command fails the strict match")
    void extraCommandKeyFailsMatch() {
        BsonDocument decoded = WireMessageDecoder.decode(opMsg(0, section0("{ping: 1, container: {orchestrator: 'kubernetes'}}")));
        BsonDocument expected = WireMessageMatcher.expand(BsonDocument.parse("{opcode: 'OP_MSG', responseTo: 0, flags: [], command: {ping: 1}}"));
        AssertionError error = assertThrows(AssertionError.class, () -> matcher().assertValuesMatchStrict(expected, decoded));
        assertTrue(error.getMessage().contains("container"), error.getMessage());
    }

    @Test
    @DisplayName("an OP_MSG without exactly one kind 0 section is a structural violation")
    void exactlyOneBodySection() {
        assertViolation(opMsg(0, section1("documents", "{a: 1}")), "exactly one kind 0 section, but this one has 0");
        assertViolation(opMsg(0, concat(section0("{ping: 1}"), section0("{ping: 2}"))), "exactly one kind 0 section, but this one has 2");
    }

    @Test
    @DisplayName("two kind 1 sections with the same identifier are a structural violation")
    void duplicateDocumentSequenceIdentifier() {
        assertViolation(opMsg(0, concat(section0("{insert: 'c'}"), section1("documents", "{a: 1}"), section1("documents", "{a: 2}"))),
                "two kind 1 sections have the identifier 'documents'");
    }

    @Test
    @DisplayName("more than one kind 3 section is a structural violation")
    void atMostOneTelemetrySection() {
        String traceContext = "{otel: {traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'}}";
        assertViolation(opMsg(0, concat(section0("{ping: 1}"), section3(traceContext), section3(traceContext))),
                "at most one kind 3 section, but this one has 2");
    }

    @Test
    @DisplayName("the section counts of the message inside an OP_COMPRESSED are checked too")
    void compressedSectionCounts() throws IOException {
        byte[] inner = opMsg(0, concat(section0("{ping: 1}"), section3("{otel: {}}"), section3("{otel: {}}")));
        byte[] innerBody = new byte[inner.length - 16];
        System.arraycopy(inner, 16, innerBody, 0, innerBody.length);
        assertViolation(compress(innerBody, innerBody.length), "at most one kind 3 section");
    }

    @Test
    @DisplayName("a header length that disagrees with the bytes written is a structural violation")
    void wrongHeaderLength() {
        byte[] bytes = opMsg(0, section0("{ping: 1}"));
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putInt(0, bytes.length + 3);
        assertViolation(bytes, "header says the message is");
    }

    @Test
    @DisplayName("bytes after the last section are a structural violation")
    void trailingBytes() {
        byte[] bytes = opMsg(0, concat(section0("{ping: 1}"), new byte[] {7, 7, 7}));
        assertViolation(bytes, "Unknown OP_MSG section kind");
    }

    @Test
    @DisplayName("a kind 1 section whose size overruns the message is a structural violation")
    void sectionSizeOverrun() {
        byte[] section = section1("telemetry");
        ByteBuffer.wrap(section).order(ByteOrder.LITTLE_ENDIAN).putInt(1, 9999);
        assertViolation(opMsg(0, concat(section0("{ping: 1}"), section)), "does not fit in the message");
    }

    @Test
    @DisplayName("a checksum is reported as a field and is not verified, so a test can assert that there is none")
    void checksumIsReported() {
        // the flag is set and four bytes follow the section; the value is not a valid CRC and is accepted anyway
        BsonDocument decoded = WireMessageDecoder.decode(opMsg(1, concat(section0("{ping: 1}"), new byte[] {1, 2, 3, 4})));
        assertEquals(new BsonArray(singletonList(new BsonString("checksumPresent"))), decoded.get("flags"));
        assertEquals(0x04030201L, decoded.getInt64("checksum").getValue());
        assertEquals(1, decoded.getArray("sections").size());

        BsonDocument expectedWithoutChecksum = WireMessageMatcher.expand(
                BsonDocument.parse("{opcode: 'OP_MSG', responseTo: 0, flags: [], command: {ping: 1}}"));
        assertThrows(AssertionError.class, () -> matcher().assertValuesMatchStrict(expectedWithoutChecksum, decoded));
    }

    @Test
    @DisplayName("four extra bytes without the checksumPresent flag are not a checksum")
    void extraBytesWithoutTheFlagAreNotAChecksum() {
        assertViolation(opMsg(0, concat(section0("{ping: 1}"), new byte[] {9, 9, 9, 9})), "Unknown OP_MSG section kind");
    }

    @Test
    @DisplayName("OP_COMPRESSED is inflated and decoded, and a wrong uncompressedSize is a structural violation")
    void compressed() throws IOException {
        byte[] inner = opMsg(0, section0("{hello: 1}"));
        byte[] innerBody = new byte[inner.length - 16];
        System.arraycopy(inner, 16, innerBody, 0, innerBody.length);

        BsonDocument decoded = WireMessageDecoder.decode(compress(innerBody, innerBody.length));
        assertEquals("OP_COMPRESSED", decoded.getString("opcode").getValue());
        assertEquals(2, decoded.getInt32("compressorId").getValue());
        assertEquals(BsonDocument.parse("{hello: 1}"), decoded.getDocument("message").getArray("sections").get(0).asDocument()
                .getDocument("body"));

        assertViolation(compress(innerBody, innerBody.length + 1), "uncompressed size");
    }

    private static ValueMatcher matcher() {
        return new ValueMatcher(new Entities(), new AssertionContext());
    }

    private static void assertViolation(final byte[] bytes, final String expectedText) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> WireMessageDecoder.decode(bytes));
        assertTrue(e.getMessage().contains(expectedText), e.getMessage());
    }

    private static byte[] header(final int length, final int opcode) {
        return ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putInt(length).putInt(42).putInt(0).putInt(opcode).array();
    }

    private static byte[] opMsg(final int flags, final byte[] sections) {
        ByteBuffer body = ByteBuffer.allocate(4 + sections.length).order(ByteOrder.LITTLE_ENDIAN).putInt(flags).put(sections);
        return concat(header(16 + body.capacity(), OP_MSG), body.array());
    }

    private static byte[] section0(final String json) {
        return concat(new byte[] {0}, encode(BsonDocument.parse(json)));
    }

    private static byte[] section3(final String json) {
        return concat(new byte[] {3}, encode(BsonDocument.parse(json)));
    }

    private static byte[] section1(final String identifier, final String... jsonDocuments) {
        byte[] id = (identifier + "\0").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] documents = new byte[0];
        for (String json : jsonDocuments) {
            documents = concat(documents, encode(BsonDocument.parse(json)));
        }
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + id.length + documents.length).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1).putInt(4 + id.length + documents.length).put(id).put(documents);
        return buffer.array();
    }

    private static byte[] section1(final String identifier) {
        byte[] id = (identifier + "\0").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] emptyDocument = encode(new BsonDocument());
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + id.length + emptyDocument.length).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1).putInt(4 + id.length + emptyDocument.length).put(id).put(emptyDocument);
        return buffer.array();
    }

    private static byte[] compress(final byte[] innerBody, final int claimedSize) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (DeflaterOutputStream out = new DeflaterOutputStream(compressed, new Deflater())) {
            out.write(innerBody);
        }
        ByteBuffer fixed = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN).putInt(OP_MSG).putInt(claimedSize).put((byte) 2);
        byte[] rest = concat(fixed.array(), compressed.toByteArray());
        return concat(header(16 + rest.length, OP_COMPRESSED), rest);
    }

    private static byte[] encode(final BsonDocument document) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        new org.bson.codecs.BsonDocumentCodec().encode(new org.bson.BsonBinaryWriter(buffer), document,
                org.bson.codecs.EncoderContext.builder().build());
        return buffer.toByteArray();
    }

    private static byte[] concat(final byte[]... arrays) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] array : arrays) {
            out.write(array, 0, array.length);
        }
        return out.toByteArray();
    }
}
