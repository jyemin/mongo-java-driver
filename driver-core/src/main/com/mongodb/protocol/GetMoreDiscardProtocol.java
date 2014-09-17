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

package com.mongodb.protocol;

import com.mongodb.MongoException;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ResponseBuffers;

/**
 * An implementation of the OP_GET_MORE protocol that can be used to terminate an exhaust cursor without decoding the results.
 *
 * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-get-more OP_GET_MORE
 * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 * @since 3.0
 */
public class GetMoreDiscardProtocol implements Protocol<Void> {
    private final long cursorId;
    private final int responseTo;

    /**
     * Construct an instance.
     *
     * @param cursorId   the cursorId
     * @param responseTo the value of the responseTo field for the first reply to discard
     */
    public GetMoreDiscardProtocol(final long cursorId, final int responseTo) {
        this.cursorId = cursorId;
        this.responseTo = responseTo;
    }

    @Override
    public Void execute(final Connection connection) {
        long curCursorId = cursorId;
        int curResponseTo = responseTo;
        while (curCursorId != 0) {
            ResponseBuffers responseBuffers = connection.receiveMessage(curResponseTo);
            try {
                curCursorId = responseBuffers.getReplyHeader().getCursorId();
                curResponseTo = responseBuffers.getReplyHeader().getRequestId();
            } finally {
                responseBuffers.close();
            }
        }
        return null;
    }

    @Override
    public MongoFuture<Void> executeAsync(final Connection connection) {
        SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();

        if (cursorId == 0) {
            retVal.init(null, null);
        } else {
            connection.receiveMessageAsync(responseTo, new DiscardCallback(connection, retVal));
        }

        return retVal;
    }

    private class DiscardCallback implements SingleResultCallback<ResponseBuffers> {
        private final Connection connection;
        private final SingleResultFuture<Void> future;

        public DiscardCallback(final Connection connection, final SingleResultFuture<Void> future) {
            this.connection = connection;
            this.future = future;
        }

        @Override
        public void onResult(final ResponseBuffers result, final MongoException e) {
            if (e != null) {
                future.init(null, e);
            } else if (result.getReplyHeader().getCursorId() == 0) {
                result.close();
                future.init(null, null);
            } else {
                connection.receiveMessageAsync(responseTo, this);
            }
        }
    }

}
