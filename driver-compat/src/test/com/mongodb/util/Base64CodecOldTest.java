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
 */

package com.mongodb.util;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


/**
 * @author Sjoerd Mulder
 */
public class Base64CodecOldTest {

    private final byte[] allBytes = new byte[255];
    private final byte[] abc = new byte[]{97, 98, 99};
    private final byte[] abcd = new byte[]{97, 98, 99, 100};
    private final byte[] abcde = new byte[]{97, 98, 99, 100, 101};

    {
        for (byte b = -128; b != 127; b++) {
            allBytes[b + 128] = b;
        }
    }

    @Test
    public void testEncode() {
        Base64Codec codec = new Base64Codec();
        assertEquals("YWJj", codec.encode(abc));
        assertEquals("YWJjZA==", codec.encode(abcd));
        assertEquals("YWJjZGU=", codec.encode(abcde));
    }

    @Test
    public void testDecode() {
        Base64Codec codec = new Base64Codec();
        assertArrayEquals(abc, codec.decode("YWJj"));
        assertArrayEquals(abcd, codec.decode("YWJjZA=="));
        assertArrayEquals(abcde, codec.decode("YWJjZGU="));
    }

    @Test
    public void testDecodeEncode() throws Exception {
        Base64Codec codec = new Base64Codec();
        assertArrayEquals(abc, codec.decode(codec.encode(abc)));
        assertArrayEquals(abcd, codec.decode(codec.encode(abcd)));
        assertArrayEquals(abcde, codec.decode(codec.encode(abcde)));
        assertArrayEquals(allBytes, codec.decode(codec.encode(allBytes)));
    }

}
