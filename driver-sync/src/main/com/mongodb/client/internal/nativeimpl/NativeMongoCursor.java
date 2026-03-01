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
package com.mongodb.client.internal.nativeimpl;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeSyncCursor;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of MongoCursor that wraps NativeSyncCursor.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeMongoCursor<T> implements MongoCursor<T> {

    private final NativeSyncCursor<T> nativeCursor;

    public NativeMongoCursor(NativeSyncCursor<T> nativeCursor) {
        this.nativeCursor = notNull("nativeCursor", nativeCursor);
    }

    @Override
    public void close() {
        nativeCursor.close();
    }

    @Override
    public boolean hasNext() {
        return nativeCursor.hasNext();
    }

    @Override
    public T next() {
        return nativeCursor.next();
    }

    @Override
    public int available() {
        // Native cursor doesn't expose this, return 0 (unknown)
        return 0;
    }

    @Override
    @Nullable
    public T tryNext() {
        // Try to get next without blocking - native cursor may not support this
        if (hasNext()) {
            return next();
        }
        return null;
    }

    @Override
    @Nullable
    public ServerCursor getServerCursor() {
        // TODO: Native cursor doesn't expose server cursor info
        return null;
    }

    @Override
    public ServerAddress getServerAddress() {
        // TODO: Get server address from native cursor when available
        return new ServerAddress();
    }
}

