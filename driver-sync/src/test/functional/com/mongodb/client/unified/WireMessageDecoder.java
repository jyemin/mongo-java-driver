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
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Decodes the bytes of one wire protocol request into the document form that {@code expectWireMessages} matches against.
 *
 * <p>This deliberately does not use any driver code that builds, parses or compresses messages. It reads the layout that the
 * specifications define, so that a bug in the driver's encoding cannot also be a bug in this decoder.</p>
 *
 * <p>It throws {@link IllegalArgumentException} for any structural violation: a wrong length, trailing bytes, an unknown opcode,
 * or a wrong uncompressed size.</p>
 */
final class WireMessageDecoder {
    private static final int HEADER_LENGTH = 16;
    private static final int OP_QUERY = 2004;
    private static final int OP_COMPRESSED = 2012;
    private static final int OP_MSG = 2013;

    static BsonDocument decode(final byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        require(bytes.length >= HEADER_LENGTH, "message is shorter than a header: " + bytes.length + " bytes");
        int messageLength = buffer.getInt();
        require(messageLength == bytes.length, "header says the message is " + messageLength + " bytes, but " + bytes.length
                + " bytes were written");
        buffer.getInt(); // requestId: arbitrary, not part of the decoded form
        int responseTo = buffer.getInt();
        int opcode = buffer.getInt();

        BsonDocument result = new BsonDocument();
        result.put("opcode", new BsonString(opcodeName(opcode)));
        result.put("responseTo", new BsonInt32(responseTo));
        if (opcode == OP_COMPRESSED) {
            result.putAll(decodeCompressed(buffer));
        } else {
            result.putAll(decodeBody(opcode, buffer));
        }
        return result;
    }

    private static BsonDocument decodeCompressed(final ByteBuffer buffer) {
        int originalOpcode = buffer.getInt();
        int uncompressedSize = buffer.getInt();
        int compressorId = buffer.get() & 0xFF;
        byte[] compressed = new byte[buffer.remaining()];
        buffer.get(compressed);

        byte[] body;
        switch (compressorId) {
            case 0:
                body = compressed;
                break;
            case 2:
                body = inflate(compressed, uncompressedSize);
                break;
            default:
                throw new IllegalArgumentException("This runner cannot decompress compressorId " + compressorId
                        + " independently of the driver; only noop (0) and zlib (2) are supported");
        }
        require(body.length == uncompressedSize, "header says the uncompressed size is " + uncompressedSize + ", but decompressing"
                + " produced " + body.length + " bytes");

        BsonDocument result = new BsonDocument();
        result.put("originalOpcode", new BsonString(opcodeName(originalOpcode)));
        result.put("uncompressedSize", new BsonInt32(uncompressedSize));
        result.put("compressorId", new BsonInt32(compressorId));
        // The wrapped message is the original message without its header.
        BsonDocument message = decodeBody(originalOpcode, ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN));
        result.put("message", message);
        return result;
    }

    private static BsonDocument decodeBody(final int opcode, final ByteBuffer buffer) {
        switch (opcode) {
            case OP_MSG:
                return decodeOpMsg(buffer);
            case OP_QUERY:
                return decodeOpQuery(buffer);
            default:
                throw new IllegalArgumentException("Unsupported opcode " + opcode);
        }
    }

    private static BsonDocument decodeOpMsg(final ByteBuffer buffer) {
        int flagBits = buffer.getInt();
        boolean checksumPresent = (flagBits & 1) != 0;
        int sectionsEnd = buffer.limit() - (checksumPresent ? 4 : 0);
        require(sectionsEnd >= buffer.position(), "message is too short to hold the checksum that checksumPresent promises");

        BsonDocument result = new BsonDocument();
        result.put("flags", flagNames(flagBits));
        BsonArray sections = new BsonArray();
        while (buffer.position() < sectionsEnd) {
            int kind = buffer.get() & 0xFF;
            BsonDocument section = new BsonDocument("kind", new BsonInt32(kind));
            if (kind == 0 || kind == 3) {
                // kind 3 is the telemetry section, a single document like kind 0
                section.put("body", readDocument(buffer, sectionsEnd));
            } else if (kind == 1) {
                int sectionStart = buffer.position();
                int size = buffer.getInt();
                int end = sectionStart + size;
                require(size >= 5 && end <= sectionsEnd, "a kind 1 section claims " + size + " bytes, which does not fit in the message");
                section.put("identifier", new BsonString(readCString(buffer, end)));
                BsonArray documents = new BsonArray();
                while (buffer.position() < end) {
                    documents.add(readDocument(buffer, end));
                }
                require(buffer.position() == end, "a kind 1 section does not end where its size says it does");
                section.put("documents", documents);
            } else {
                throw new IllegalArgumentException("Unknown OP_MSG section kind " + kind);
            }
            sections.add(section);
        }
        require(buffer.position() == sectionsEnd, "bytes remain after the last section");
        requireValidSectionCounts(sections);
        if (checksumPresent) {
            // The OP_MSG spec reserves the checksum and no driver writes one. It is reported, so that a test can assert that there
            // is none, and it is not verified.
            result.put("checksum", new BsonInt64(buffer.getInt() & 0xFFFFFFFFL));
        }
        result.put("sections", sections);
        return result;
    }

    /**
     * The OP_MSG spec requires exactly one payload type 0 section, kind 1 sections with different identifiers, and at most one
     * payload type 3 section.
     */
    private static void requireValidSectionCounts(final BsonArray sections) {
        int bodies = 0;
        int telemetry = 0;
        Set<String> identifiers = new HashSet<>();
        for (BsonValue value : sections) {
            BsonDocument section = value.asDocument();
            int kind = section.getInt32("kind").getValue();
            if (kind == 0) {
                bodies++;
            } else if (kind == 3) {
                telemetry++;
            } else if (kind == 1) {
                String identifier = section.getString("identifier").getValue();
                require(identifiers.add(identifier), "two kind 1 sections have the identifier '" + identifier + "'");
            }
        }
        require(bodies == 1, "an OP_MSG must have exactly one kind 0 section, but this one has " + bodies);
        require(telemetry <= 1, "an OP_MSG must have at most one kind 3 section, but this one has " + telemetry);
    }

    private static BsonDocument decodeOpQuery(final ByteBuffer buffer) {
        int flagBits = buffer.getInt();
        String fullCollectionName = readCString(buffer, buffer.limit());
        int numberToSkip = buffer.getInt();
        int numberToReturn = buffer.getInt();
        BsonDocument result = new BsonDocument();
        result.put("flags", opQueryFlagNames(flagBits));
        result.put("fullCollectionName", new BsonString(fullCollectionName));
        result.put("numberToSkip", new BsonInt32(numberToSkip));
        result.put("numberToReturn", new BsonInt32(numberToReturn));
        result.put("query", readDocument(buffer, buffer.limit()));
        if (buffer.hasRemaining()) {
            result.put("returnFieldsSelector", readDocument(buffer, buffer.limit()));
        }
        require(!buffer.hasRemaining(), "bytes remain after the OP_QUERY documents");
        return result;
    }

    private static BsonDocument readDocument(final ByteBuffer buffer, final int limit) {
        int start = buffer.position();
        require(limit - start >= 5, "not enough bytes for a document");
        int size = buffer.getInt(start);
        require(size >= 5 && start + size <= limit, "a document claims " + size + " bytes, which does not fit");
        byte[] documentBytes = new byte[size];
        buffer.get(documentBytes);
        require(documentBytes[size - 1] == 0, "a document does not end with a zero byte");
        try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(documentBytes).order(ByteOrder.LITTLE_ENDIAN))) {
            BsonDocument document = new BsonDocumentCodec().decode(reader, DecoderContext.builder().build());
            return document;
        }
    }

    private static String readCString(final ByteBuffer buffer, final int limit) {
        int start = buffer.position();
        int end = start;
        while (end < limit && buffer.get(end) != 0) {
            end++;
        }
        require(end < limit, "a C string is not terminated");
        byte[] bytes = new byte[end - start];
        buffer.get(bytes);
        buffer.get(); // the terminator
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] inflate(final byte[] compressed, final int expectedSize) {
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(compressed);
            byte[] out = new byte[expectedSize + 1];
            int total = 0;
            while (!inflater.finished()) {
                int n = inflater.inflate(out, total, out.length - total);
                if (n == 0 && (inflater.needsInput() || inflater.needsDictionary())) {
                    break;
                }
                total += n;
                require(total <= expectedSize, "decompressing produced more than the " + expectedSize + " bytes the header promises");
            }
            require(inflater.finished(), "the zlib stream is truncated");
            byte[] result = new byte[total];
            System.arraycopy(out, 0, result, 0, total);
            return result;
        } catch (DataFormatException e) {
            throw new IllegalArgumentException("invalid zlib data: " + e.getMessage(), e);
        } finally {
            inflater.end();
        }
    }

    private static BsonArray flagNames(final int flagBits) {
        BsonArray names = new BsonArray();
        for (int bit = 0; bit < 32; bit++) {
            if ((flagBits & (1 << bit)) != 0) {
                names.add(new BsonString(bit == 0 ? "checksumPresent" : bit == 1 ? "moreToCome" : bit == 16 ? "exhaustAllowed"
                        : "unknownBit" + bit));
            }
        }
        return names;
    }

    private static BsonArray opQueryFlagNames(final int flagBits) {
        String[] names = {null, "tailableCursor", "secondaryOk", "oplogReplay", "noCursorTimeout", "awaitData", "exhaust", "partial"};
        BsonArray result = new BsonArray();
        for (int bit = 0; bit < 32; bit++) {
            if ((flagBits & (1 << bit)) != 0) {
                result.add(new BsonString(bit < names.length && names[bit] != null ? names[bit] : "unknownBit" + bit));
            }
        }
        return result;
    }

    private static String opcodeName(final int opcode) {
        switch (opcode) {
            case OP_QUERY:
                return "OP_QUERY";
            case OP_COMPRESSED:
                return "OP_COMPRESSED";
            case OP_MSG:
                return "OP_MSG";
            default:
                throw new IllegalArgumentException("Unknown opcode " + opcode);
        }
    }

    private static void require(final boolean condition, final String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    private WireMessageDecoder() {
    }
}
