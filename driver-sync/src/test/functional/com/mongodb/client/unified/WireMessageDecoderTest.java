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
    @DisplayName("a wrong checksum is a structural violation and a right one is accepted")
    void checksum() {
        byte[] withGoodChecksum = opMsgWithChecksum(section0("{ping: 1}"), false);
        assertEquals(new BsonArray(singletonList(new BsonString("checksumPresent"))), WireMessageDecoder.decode(withGoodChecksum).get("flags"));
        assertViolation(opMsgWithChecksum(section0("{ping: 1}"), true), "CRC-32C");
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

    private static byte[] opMsgWithChecksum(final byte[] sections, final boolean corrupt) {
        ByteBuffer body = ByteBuffer.allocate(4 + sections.length + 4).order(ByteOrder.LITTLE_ENDIAN).putInt(1).put(sections);
        byte[] withoutChecksum = concat(header(16 + body.capacity(), OP_MSG), java.util.Arrays.copyOf(body.array(), body.capacity() - 4));
        int crc = crc32c(withoutChecksum);
        ByteBuffer.wrap(body.array()).order(ByteOrder.LITTLE_ENDIAN).putInt(body.capacity() - 4, corrupt ? ~crc : crc);
        return concat(header(16 + body.capacity(), OP_MSG), body.array());
    }

    private static byte[] section0(final String json) {
        return concat(new byte[] {0}, encode(BsonDocument.parse(json)));
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

    private static int crc32c(final byte[] bytes) {
        int crc = ~0;
        for (byte b : bytes) {
            crc ^= b & 0xFF;
            for (int bit = 0; bit < 8; bit++) {
                crc = (crc >>> 1) ^ (0x82F63B78 & -(crc & 1));
            }
        }
        return ~crc;
    }
}
