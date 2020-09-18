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

import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.Stream;
import com.mongodb.internal.timeout.Deadline;
import org.bson.ByteBuf;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

public class SocketStream implements Stream {
    private final ServerAddress address;
    private final SocketSettings settings;
    private final SslSettings sslSettings;
    private final SocketFactory socketFactory;
    private final BufferProvider bufferProvider;
    private volatile Socket socket;
    private volatile OutputStream outputStream;
    private volatile InputStream inputStream;
    private volatile boolean isClosed;

    public SocketStream(final ServerAddress address, final SocketSettings settings, final SslSettings sslSettings,
                        final SocketFactory socketFactory, final BufferProvider bufferProvider) {
        this.address = notNull("address", address);
        this.settings = notNull("settings", settings);
        this.sslSettings = notNull("sslSettings", sslSettings);
        this.socketFactory = notNull("socketFactory", socketFactory);
        this.bufferProvider = notNull("bufferProvider", bufferProvider);
    }

    @Override
    public void open() {
        open(0);
    }

    @Override
    public boolean supportsOpenTimeout() {
        return true;
    }

    @Override
    public void open(final int timeoutMS) {
        try {
            socket = initializeSocket(timeoutMS == 0 ? Deadline.infinite() : Deadline.finite(timeoutMS, TimeUnit.MILLISECONDS));
            outputStream = socket.getOutputStream();
            inputStream = socket.getInputStream();
        } catch (IOException e) {
            close();
            throw new MongoSocketOpenException("Exception opening socket", getAddress(), e);
        }
    }

    protected Socket initializeSocket(final Deadline deadline) throws IOException {
        Iterator<InetSocketAddress> inetSocketAddresses = address.getSocketAddresses().iterator();
        while (inetSocketAddresses.hasNext()) {
            Socket socket = socketFactory.createSocket();
            try {
                SocketStreamHelper.initialize(socket, inetSocketAddresses.next(), settings, sslSettings, deadline);
                return socket;
            } catch (SocketTimeoutException e) {
                if (!inetSocketAddresses.hasNext()) {
                    throw e;
                }
            }
        }

        throw new MongoSocketException("Exception opening socket", getAddress());
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return bufferProvider.getBuffer(size);
    }

    // TODO: blocking write operations using SocketOutputStream don't accept a timeout.  The only way is to use non-blocking I/O.
    // TODO: see https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4031100
    @Override
    public boolean supportsWriteTimeout() {
        return false;
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        for (final ByteBuf cur : buffers) {
            outputStream.write(cur.array(), 0, cur.limit());
        }
    }

    @Override
    public ByteBuf read(final int numBytes) throws IOException {
        return readWithTimeout(numBytes, 0);
    }

    @Override
    public boolean supportsReadTimeout() {
        return true;
    }

    @Override
    public ByteBuf readWithTimeout(final int numBytes, final int timeoutMS) throws IOException {
        Deadline deadline = Deadline.min(Deadline.of(timeoutMS, TimeUnit.MILLISECONDS),
                Deadline.of(socket.getSoTimeout(), TimeUnit.MILLISECONDS));
        ByteBuf buffer = bufferProvider.getBuffer(numBytes);
        int totalBytesRead = 0;
        byte[] bytes = buffer.array();
        while (totalBytesRead < buffer.limit()) {
            // TODO: dangerous cast from long to int?
            socket.setSoTimeout((int) deadline.getTimeRemaining(TimeUnit.MILLISECONDS));
            int bytesRead = inputStream.read(bytes, totalBytesRead, buffer.limit() - totalBytesRead);
            if (bytesRead == -1) {
                buffer.release();
                throw new MongoSocketReadException("Prematurely reached end of stream", getAddress());
            }
            totalBytesRead += bytesRead;
        }
        return buffer;
    }

    @Override
    public boolean supportsAdditionalTimeout() {
        return true;
    }

    @Override
    public ByteBuf read(final int numBytes, final int additionalTimeout) throws IOException {
        int curTimeout = socket.getSoTimeout();
        if (curTimeout > 0 && additionalTimeout > 0) {
            socket.setSoTimeout(curTimeout + additionalTimeout);
        }
        try {
            return read(numBytes);
        } finally {
            socket.setSoTimeout(curTimeout);
        }
    }

    @Override
    public void openAsync(final AsyncCompletionHandler<Void> handler) {
        throw new UnsupportedOperationException(getClass() + " does not support asynchronous operations.");
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
        throw new UnsupportedOperationException(getClass() + " does not support asynchronous operations.");
    }

    @Override
    public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
        throw new UnsupportedOperationException(getClass() + " does not support asynchronous operations.");
    }

    @Override
    public ServerAddress getAddress() {
        return address;
    }

    /**
     * Get the settings for this socket.
     *
     * @return the settings
     */
    SocketSettings getSettings() {
        return settings;
    }

    @Override
    public void close() {
        try {
            isClosed = true;
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e) {
            // ignore
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}
