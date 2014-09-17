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

package com.mongodb.protocol.message;

import com.mongodb.operation.GetMore;
import org.bson.io.BsonOutput;

public class GetMoreMessage extends RequestMessage {
    private final GetMore getMore;

    public GetMoreMessage(final String collectionName, final GetMore getMore) {
        super(collectionName, OpCode.OP_GETMORE, MessageSettings.builder().build());
        this.getMore = getMore;
    }

    public long getCursorId() {
        return getMore.getServerCursor().getId();
    }

    @Override
    protected RequestMessage encodeMessageBody(final BsonOutput bsonOutput, final int messageStartPosition) {
        writeGetMore(bsonOutput);
        return null;
    }

    private void writeGetMore(final BsonOutput buffer) {
        buffer.writeInt32(0);
        buffer.writeCString(getCollectionName());
        buffer.writeInt32(getMore.getNumberToReturn());
        buffer.writeInt64(getMore.getServerCursor().getId());
    }

}
