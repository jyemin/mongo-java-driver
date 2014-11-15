package com.mongodb.operation;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.async.SingleResultCallback;

import java.io.Closeable;
import java.util.List;

/**
 * An asynchronous iterator over the batches provided by a MongoDB cursor.
 *
 * @since 3.0
 *
 * @param <T> The type of documents the cursor contains
 */
public interface AsyncBatchCursor<T> extends Closeable {
    /**
     * Returns the next batch of results.  A tailable cursor will block until another batch exists.
     *
     * @param callback callback to receive the next batch of results
     * @throws java.util.NoSuchElementException if no next batch exists
     */
    void next(SingleResultCallback<List<T>> callback);

    /**
     * Sets the batch size to use when requesting the next batch.  This is the number of documents to request in the next batch.
     *
     * @param batchSize the non-negative batch size.  0 means to use the server default.
     */
    void setBatchSize(int batchSize);

    /**
     * Gets the batch size to use when requesting the next batch.  This is the number of documents to request in the next batch.
     *
     * @return the non-negative batch size.  0 means to use the server default.
     */
    int getBatchSize();

    @Override
    void close();
}
