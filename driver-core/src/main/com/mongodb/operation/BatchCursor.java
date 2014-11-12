package com.mongodb.operation;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.annotations.NotThreadSafe;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

/**
 * An iterator over the batches provided by a MongoDB cursor.
 *
 * @since 3.0
 *
 * @param <T> The type of documents the cursor contains
 */
@NotThreadSafe
public interface BatchCursor<T> extends Iterator<List<T>>, Closeable {
    @Override
    void close();

    /**
     * A special {@code next()} case that returns the next batch if available or null.
     *
     * <p>Tailable cursors are an example where this is useful. A call to {@code tryNext()} may return null, but in the future calling
     * {@code tryNext()} would return a new batch if a document had been added to the capped collection.</p>
     *
     * @return the next batch if available or null.
     * @mongodb.driver.manual reference/glossary/#term-tailable-cursor Tailable Cursor
     */
    List<T> tryNext();

    /**
     * Returns the server cursor
     *
     * @return ServerCursor
     */
    ServerCursor getServerCursor();

    /**
     * Returns the server address
     *
     * @return ServerAddress
     */
    ServerAddress getServerAddress();
}
