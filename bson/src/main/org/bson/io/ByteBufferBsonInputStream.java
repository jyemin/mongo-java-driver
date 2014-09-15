/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.bson.io;

import org.bson.ByteBuf;
import org.bson.types.ObjectId;

import java.nio.ByteOrder;
import java.nio.charset.Charset;

/**
 * An implementation of {@code BsonInputStream} that is backed by a {@code ByteBuf}.
 * @since 3.0
 */
public class ByteBufferBsonInputStream implements BsonInputStream {
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    private ByteBuf buffer;
    private int mark = -1;

    /**
     * Construct an instance with the given byte buffer.  The stream takes over ownership of the buffer and closes it when this instance
     * is closed.
     * @param buffer the byte buffer
     */
    public ByteBufferBsonInputStream(final ByteBuf buffer) {
        this.buffer = buffer;
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public int getPosition() {
        return buffer.position();
    }

    @Override
    public byte readByte() {
        return buffer.get();
    }

    @Override
    public void readBytes(final byte[] bytes) {
        buffer.get(bytes);
    }

    @Override
    public void readBytes(final byte[] bytes, final int offset, final int length) {
       buffer.get(bytes, offset, length);
    }

    @Override
    public long readInt64() {
        return buffer.getLong();
    }

    @Override
    public double readDouble() {
        return buffer.getDouble();
    }

    @Override
    public int readInt32() {
        return buffer.getInt();
    }

    @Override
    public String readString() {
        int size = readInt32();
        byte[] bytes = new byte[size];
        readBytes(bytes);
        return new String(bytes, 0, size - 1, UTF8_CHARSET);
    }

    @Override
    public ObjectId readObjectId() {
        byte[] bytes = new byte[12];
        readBytes(bytes);
        return new ObjectId(bytes);
    }

    @Override
    public String readCString() {
        // TODO: potentially optimize this
        int mark = buffer.position();
        readUntilNullByte();
        int size = buffer.position() - mark - 1;
        buffer.position(mark);

        byte[] bytes = new byte[size];
        readBytes(bytes);
        readByte();  // read the trailing null byte

        return new String(bytes, UTF8_CHARSET);
    }

    private void readUntilNullByte() {
        //CHECKSTYLE:OFF
        while (buffer.get() != 0) { //NOPMD
            //do nothing - checkstyle & PMD hate this, not surprisingly
        }
        //CHECKSTYLE:ON
    }

    @Override
    public void skipCString() {
        readUntilNullByte();
    }

    @Override
    public void skip(final int numBytes) {
        buffer.position(buffer.position() + numBytes);
    }

    @Override
    public void mark(final int readLimit) {
        mark = buffer.position();
    }

    @Override
    public void reset() {
        if (mark == -1) {
            throw new IllegalStateException("Mark not set");
        }
        buffer.position(mark);
    }

    @Override
    public boolean hasRemaining() {
        return !buffer.hasRemaining();
    }

    public void close() {
        buffer.close();
        buffer = null;
    }
}
