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
 *
 * Originally from https://github.com/marianobarrios/tls-channel
 *
 * Copyright (c) [2015-2018] all contributors
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.mongodb.internal.connection.tlschannel;

/**
 * This exception signals the caller that the operation could not continue
 * because a CPU-intensive operation (typically a TLS handshaking) needs to be
 * executed and the {@link TlsChannel} is configured to not run tasks.
 * This allows the application to run these tasks in some other threads, in
 * order to not slow the selection loop. The method that threw the exception
 * should be retried once the task supplied by {@link #getTask()} is executed
 * and finished.
 * <p>
 * This exception is akin to the SSL_ERROR_WANT_ASYNC error code used by OpenSSL
 * (but note that in OpenSSL, the task is executed by the library, while with
 * the {@link TlsChannel}, the calling code is responsible for the
 * execution).
 *
 * @see <a href="https://www.openssl.org/docs/man1.1.0/ssl/SSL_get_error.html">
 * OpenSSL error documentation</a>
 */
public class NeedsTaskException extends TlsChannelFlowControlException {

    private static final long serialVersionUID = -7869448883241411803L;

    private Runnable task;

    public NeedsTaskException(final Runnable task) {
        this.task = task;
    }

    public Runnable getTask() {
        return task;
    }

}
