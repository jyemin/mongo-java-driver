/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.UUID;

public class ByteBufferInputBenchmark {
    public static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    @Rule
    public MethodRule benchmarkRun = new BenchmarkRule();

    private static ByteBuffer buffer;
    private static int count;

    @BeforeClass
    public static void setup() {
        buffer = ByteBuffer.allocateDirect(99999999);

        byte[] bytes = "abcdefghij".getBytes(UTF8_CHARSET);
        count = buffer.capacity() / (bytes.length + 1);

        int i = 0;
        while (i++ < count) {
            buffer.put(bytes);
            buffer.put((byte) 0);
        }
        buffer.flip();
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 20)
    public void benchmarkReadCString() throws UnsupportedEncodingException {
        ByteBufferInput inputBuffer = new ByteBufferInput(buffer);
        int i = 0;
        while (i++ < count){
            inputBuffer.readCString();
        }
        buffer.rewind();
    }

}
