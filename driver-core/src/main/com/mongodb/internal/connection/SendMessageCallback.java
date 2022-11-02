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

import com.mongodb.RequestContext;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.connection.message.RequestMessage;
import org.bson.io.OutputBuffer;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ProtocolHelper.sendCommandFailedEvent;

class SendMessageCallback<T> implements SingleResultCallback<Void> {
    private final OutputBuffer buffer;
    private final InternalConnection connection;
    private final SingleResultCallback<ResponseBuffers> receiveMessageCallback;
    private final int requestId;
    private final RequestMessage message;
    private final CommandListener commandListener;
    private final long startTimeNanos;
    private final RequestContext requestContext;
    private final SingleResultCallback<T> callback;
    private final String commandName;

    SendMessageCallback(final InternalConnection connection, final OutputBuffer buffer, final RequestMessage message,
                        final String commandName, final long startTimeNanos, final CommandListener commandListener,
                        final RequestContext requestContext,
                        final SingleResultCallback<T> callback, final SingleResultCallback<ResponseBuffers> receiveMessageCallback) {
        this.buffer = buffer;
        this.connection = connection;
        this.message = message;
        this.commandName = commandName;
        this.commandListener = commandListener;
        this.startTimeNanos = startTimeNanos;
        this.requestContext = notNull("requestContext", requestContext);
        this.callback = callback;
        this.receiveMessageCallback = receiveMessageCallback;
        this.requestId = message.getId();
    }

    @Override
    public void onResult(final Void result, final Throwable t) {
        buffer.close();
        if (t != null) {
            if (commandListener != null){
                sendCommandFailedEvent(message, commandName, connection.getDescription(), System.nanoTime() - startTimeNanos, t,
                        commandListener, requestContext);
            }
            callback.onResult(null, t);
        } else {
            connection.receiveMessageAsync(requestId, receiveMessageCallback);
        }
    }
}
