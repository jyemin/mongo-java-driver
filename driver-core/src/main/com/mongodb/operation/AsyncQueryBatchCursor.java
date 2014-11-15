package com.mongodb.operation;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import org.bson.codecs.Decoder;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CursorHelper.getNumberToReturn;
import static java.util.Arrays.asList;

class AsyncQueryBatchCursor<T> implements AsyncBatchCursor<T> {
    private final MongoNamespace namespace;
    private final int limit;
    private final Decoder<T> decoder;
    private final AsyncConnectionSource connectionSource;
    private volatile QueryResult<T> firstBatch;
    private volatile int batchSize;
    private volatile ServerCursor cursor;
    private volatile int count;
    private volatile boolean closed;

    AsyncQueryBatchCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final int limit, final int batchSize,
                          final Decoder<T> decoder, final AsyncConnectionSource connectionSource) {
        this.namespace = namespace;
        this.firstBatch = firstBatch;
        this.limit = limit;
        this.batchSize = batchSize;
        this.decoder = decoder;
        this.cursor = firstBatch.getCursor();
        this.connectionSource = notNull("connectionSource", connectionSource).retain();
        this.count += firstBatch.getResults().size();
        if (limitReached()) {
            killCursor();
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            killCursor();
        }
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        isTrue("open", !closed);
        if (firstBatch != null) {
            List<T> results = firstBatch.getResults();
            firstBatch = null;
            callback.onResult(results, null);
        } else {
            if (cursor == null) {
                close();
                callback.onResult(null, null);
            } else {
                getMore(callback);
            }
        }
    }

    @Override
    public void setBatchSize(final int batchSize) {
        isTrue("open", !closed);
        this.batchSize = batchSize;
    }

    @Override
    public int getBatchSize() {
        isTrue("open", !closed);
        return batchSize;
    }

    private boolean limitReached() {
        return limit != 0 && count >= limit;
    }

    private void getMore(final SingleResultCallback<List<T>> callback) {
        connectionSource.getConnection().register(new SingleResultCallback<Connection>() {
            @Override
            public void onResult(final Connection connection, final MongoException e) {
                if (e != null) {
                    callback.onResult(null, e);
                } else {
                    connection.getMoreAsync(namespace, cursor.getId(), getNumberToReturn(limit, batchSize, count),
                                            decoder)
                              .register(new QueryResultSingleResultCallback(connection, callback));
                }
            }
        });
    }

    private void killCursor() {
        if (cursor != null) {
            final ServerCursor localCursor = cursor;
            cursor = null;
            connectionSource.getConnection().register(new SingleResultCallback<Connection>() {
                @Override
                public void onResult(final Connection connection, final MongoException connectionException) {
                    connection.killCursorAsync(asList(localCursor.getId()))
                              .register(new SingleResultCallback<Void>() {
                                  @Override
                                  public void onResult(final Void result, final MongoException killException) {
                                      connection.release();
                                      connectionSource.release();
                                  }
                              });
                }
            });
        } else {
            connectionSource.release();
        }
    }

    private class QueryResultSingleResultCallback implements SingleResultCallback<QueryResult<T>> {
        private final Connection connection;
        private final SingleResultCallback<List<T>> callback;

        public QueryResultSingleResultCallback(final Connection connection, final SingleResultCallback<List<T>> callback) {
            this.connection = connection;
            this.callback = callback;
        }

        @Override
        public void onResult(final QueryResult<T> result, final MongoException e) {
            if (e != null) {
                connection.release();
                close();
                callback.onResult(null, e);
            } else if (result.getResults().isEmpty() && result.getCursor() != null) {
                connection.getMoreAsync(namespace, cursor.getId(), getNumberToReturn(limit, batchSize, count),
                                        decoder)
                          .register(this);
            } else {
                cursor = result.getCursor();
                count += result.getResults().size();
                if (limitReached()) {
                    killCursor();
                }
                connection.release();
                if (result.getResults().isEmpty()) {
                    callback.onResult(null, null);
                } else {
                    callback.onResult(result.getResults(), null);
                }
            }
        }
    }
}
