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

package com.mongodb.internal.connection;

import org.bson.ByteBuf;
import org.bson.io.OutputBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ByteBufferBsonOutput extends OutputBuffer {

    private static final int INITIAL_SHIFT = 10;
    public static final int INITIAL_BUFFER_SIZE = 1 << INITIAL_SHIFT;

    private ByteBuf buffer;

    /**
     * Construct an instance that uses the given buffer provider to allocate byte buffers as needs as it grows.
     *
     * @param bufferProvider the non-null buffer provider
     */
    public ByteBufferBsonOutput(final BufferProvider bufferProvider) {
        // TODO: could lower this based on some other knowledge, like whether there is a payload.  For commands with no payload, the max
        //  size is more like 16M + 16K
        // TODO: though there are no failing tests, it seems like this hard limit would cause problem with
        //  BsonWriterHelper.writeDocument, which expects that you can write beyond the max message size and then truncate to
        //  the positionn of the previous document.  We could deal with that by some trickery, by sending any bytes past the buffer limit
        //  to /dev/null, since ultimately they will be truncated anyway.  But would have to be careful.
        buffer = bufferProvider.getBuffer(48_000_000);
    }

    @Override
    public void writeBytes(final byte[] bytes, final int offset, final int length) {
        buffer.put(bytes, 0, length);
    }

    @Override
    public void writeByte(final int value) {
        buffer.put((byte) value);
    }

    @Override
    public int getPosition() {
        return buffer.position();
    }

    @Override
    public int getSize() {
        return buffer.position();
    }

    protected void write(final int absolutePosition, final int value) {
        buffer.put(absolutePosition, (byte) value);
    }

    @Override
    public List<ByteBuf> getByteBuffers() {
        return Collections.singletonList(buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN).flip());
    }


    @Override
    public int pipe(final OutputStream out) throws IOException {
        // TODO: remove loop
        byte[] tmp = new byte[buffer.position()];

        int total = 0;
        for (final ByteBuf cur : getByteBuffers()) {
            ByteBuf dup = cur.duplicate();
            while (dup.hasRemaining()) {
                int numBytesToCopy = Math.min(dup.remaining(), tmp.length);
                dup.get(tmp, 0, numBytesToCopy);
                out.write(tmp, 0, numBytesToCopy);
            }
            total += dup.limit();
        }
        return total;
    }

    @Override
    public void truncateToPosition(final int newPosition) {
        buffer.position(newPosition);
    }

    @Override
    public void close() {
        if (buffer != null) {
            buffer.release();
            buffer = null;
        }
    }
}
