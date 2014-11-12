package com.mongodb.operation;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @since 3.0
 */
public interface BatchCursor<T> extends Iterator<List<T>>, Closeable {
    @Override
    void close();

    List<T> tryNext();

    ServerCursor getServerCursor();

    ServerAddress getServerAddress();
}
