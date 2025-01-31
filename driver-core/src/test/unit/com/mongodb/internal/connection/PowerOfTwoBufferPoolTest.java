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
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class PowerOfTwoBufferPoolTest {
    private static final int LOWEST_POOLED_POWER_OF_TWO = 4;
    private static final int HIGHEST_POOLED_POWER_OF_TWO = 10;

    private PowerOfTwoBufferPool pool;

    @Before
    public void setUp() {
        pool = new PowerOfTwoBufferPool(LOWEST_POOLED_POWER_OF_TWO, HIGHEST_POOLED_POWER_OF_TWO);
    }

    @Test
    public void testRequestForPooledBuffer() {

        for (int i = LOWEST_POOLED_POWER_OF_TWO; i <= HIGHEST_POOLED_POWER_OF_TWO; i++) {
            ByteBuf buf = pool.getBuffer((int) Math.pow(2, i));
            assertEquals((int) Math.pow(2, i), buf.capacity());
            assertEquals((int) Math.pow(2, i), buf.limit());

            if (i > LOWEST_POOLED_POWER_OF_TWO) {
                buf = pool.getBuffer((int) Math.pow(2, i) - 1);
                assertEquals((int) Math.pow(2, i), buf.capacity());
                assertEquals((int) Math.pow(2, i) - 1, buf.limit());
            }

            if (i < HIGHEST_POOLED_POWER_OF_TWO) {
                buf = pool.getBuffer((int) Math.pow(2, i) + 1);
                assertEquals((int) Math.pow(2, i + 1), buf.capacity());
                assertEquals((int) Math.pow(2, i) + 1, buf.limit());
            }
        }
    }

    @Test
    public void testReuse() {
        ByteBuf buf = pool.getBuffer((int) Math.pow(2, 10));
        ByteBuffer byteBuffer = buf.asNIO();
        buf.release();
        assertSame(byteBuffer, pool.getBuffer((int) Math.pow(2, 10)).asNIO());
    }

    @Test
    public void testRequestForUnpooledBuffer() {
        ByteBuf buf = pool.getBuffer((int) Math.pow(2, LOWEST_POOLED_POWER_OF_TWO) - 1);
        assertEquals((int) Math.pow(2, LOWEST_POOLED_POWER_OF_TWO) - 1, buf.capacity());
        assertEquals((int) Math.pow(2, LOWEST_POOLED_POWER_OF_TWO) - 1, buf.limit());

        buf.release();
        assertNotSame(buf, pool.getBuffer((int) Math.pow(2, LOWEST_POOLED_POWER_OF_TWO) + 1));

        buf = pool.getBuffer((int) Math.pow(2, HIGHEST_POOLED_POWER_OF_TWO) + 1);
        assertEquals((int) Math.pow(2, HIGHEST_POOLED_POWER_OF_TWO) + 1, buf.capacity());
        assertEquals((int) Math.pow(2, HIGHEST_POOLED_POWER_OF_TWO) + 1, buf.limit());

        buf.release();
        assertNotSame(buf, pool.getBuffer((int) Math.pow(2, HIGHEST_POOLED_POWER_OF_TWO) + 1));
    }

    // Racy test
    @Test
    public void testPruning() throws InterruptedException {
        PowerOfTwoBufferPool pool = new PowerOfTwoBufferPool(0, 10, 5, TimeUnit.MILLISECONDS)
                .enablePruning();
        try {
            ByteBuf byteBuf = pool.getBuffer(256);
            ByteBuffer wrappedByteBuf = byteBuf.asNIO();
            byteBuf.release();
            Thread.sleep(50);
            ByteBuf newByteBuf = pool.getBuffer(256);
            assertNotSame(wrappedByteBuf, newByteBuf.asNIO());
        } finally {
            pool.disablePruning();
        }
    }
}
