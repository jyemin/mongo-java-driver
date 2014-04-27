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

package org.mongodb.async;

import org.mongodb.Block;
import org.mongodb.CollectibleCodec;
import org.mongodb.ConvertibleToDocument;
import org.mongodb.Document;
import org.mongodb.Function;
import org.mongodb.MongoAsyncCursor;
import org.mongodb.MongoCollectionOptions;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.WriteResult;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.AsyncReadOperation;
import org.mongodb.operation.AsyncWriteOperation;
import org.mongodb.operation.CountOperation;
import org.mongodb.operation.Find;
import org.mongodb.operation.InsertOperation;
import org.mongodb.operation.InsertRequest;
import org.mongodb.operation.QueryOperation;
import org.mongodb.operation.ReplaceOperation;
import org.mongodb.operation.ReplaceRequest;
import org.mongodb.operation.SingleResultFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.mongodb.assertions.Assertions.notNull;

class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final MongoNamespace namespace;
    private final CollectibleCodec<T> codec;
    private final MongoCollectionOptions options;
    private final MongoClientImpl client;

    public MongoCollectionImpl(final MongoNamespace namespace, final CollectibleCodec<T> codec, final MongoCollectionOptions options,
                               final MongoClientImpl client) {

        this.namespace = namespace;
        this.codec = codec;
        this.options = options;
        this.client = client;
    }

    @Override
    public String getName() {
        return namespace.getCollectionName();
    }

    @Override
    public MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    public MongoCollectionOptions getOptions() {
        return options;
    }

    @Override
    public CollectibleCodec<T> getCodec() {
        return codec;
    }

    @Override
    public MongoView<T> find(final Document filter) {
        return new MongoCollectionView().find(filter);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MongoFuture<WriteResult> insert(final T document) {
        notNull("document", document);
        return insert(asList(document));
    }

    @Override
    public MongoFuture<WriteResult> insert(final List<T> documents) {
        notNull("documents", documents);
        List<InsertRequest<T>> insertRequests = new ArrayList<InsertRequest<T>>();
        for (T document : documents) {
            insertRequests.add(new InsertRequest<T>(document));
        }
        return execute(new InsertOperation<T>(getNamespace(), true, options.getWriteConcern(), insertRequests,
                                              getCodec()));
    }

    @Override
    public MongoFuture<WriteResult> save(final T document) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    <V> MongoFuture<V> execute(final AsyncWriteOperation<V> writeOperation) {
        return client.execute(writeOperation);
    }

    <V> MongoFuture<V> execute(final AsyncReadOperation<V> readOperation, final ReadPreference readPreference) {
        return client.execute(readOperation, readPreference);
    }

    private class MongoCollectionView implements MongoView<T> {
        private final Find find = new Find();
        private final ReadPreference readPreference = options.getReadPreference();

        @Override
        public MongoFuture<T> one() {
            final SingleResultFuture<T> retVal = new SingleResultFuture<T>();
            execute(new QueryOperation<T>(getNamespace(), find.batchSize(-1), new DocumentCodec(), getCodec()), readPreference)
            .register(new
                      SingleResultCallback<MongoAsyncCursor<T>>() {
                          @Override
                          public void onResult(final MongoAsyncCursor<T> cursor, final MongoException e) {
                              if (e != null) {
                                  retVal.init(null, e);
                              } else {
                                  cursor.forEach(new Block<T>() {
                                      @Override
                                      public void apply(final T t) {
                                          retVal.init(t, null);
                                      }
                                  });
                              }
                          }
                      });
            return retVal;
        }

        @Override
        public MongoFuture<Long> count() {
            return execute(new CountOperation(namespace, find, new DocumentCodec()), readPreference);
        }

        @Override
        public MongoView<T> find(final Document filter) {
            find.filter(filter);
            return this;
        }

        @Override
        public MongoView<T> find(final ConvertibleToDocument filter) {
            return find(filter.toDocument());
        }

        @Override
        public MongoView<T> sort(final Document sortCriteria) {
            find.order(sortCriteria);
            return this;
        }

        @Override
        public MongoView<T> sort(final ConvertibleToDocument sortCriteria) {
            return sort(sortCriteria.toDocument());
        }

        @Override
        public MongoView<T> skip(final int skip) {
            find.skip(skip);
            return this;
        }

        @Override
        public MongoView<T> limit(final int limit) {
            find.limit(limit);
            return this;
        }

        @Override
        public MongoView<T> fields(final Document selector) {
            find.select(selector);
            return this;
        }

        @Override
        public MongoView<T> fields(final ConvertibleToDocument selector) {
            return fields(selector.toDocument());
        }

        @Override
        public MongoFuture<Void> forEach(final Block<? super T> block) {
            final SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();
            execute(new QueryOperation<T>(getNamespace(), find, new DocumentCodec(), getCodec()), readPreference)
            .register(new
                      SingleResultCallback<MongoAsyncCursor<T>>() {
                          @Override
                          public void onResult(final MongoAsyncCursor<T> cursor, final MongoException e) {
                              if (e != null) {
                                  retVal.init(null, e);
                              } else {
                                  cursor.forEach(new Block<T>() {
                                      @Override
                                      public void apply(final T t) {
                                          block.apply(t);
                                      }
                                  }).register(new SingleResultCallback<Void>() {
                                      @Override
                                      public void onResult(final Void result, final MongoException e) {
                                          if (e != null) {
                                              retVal.init(null, e);
                                          } else {
                                              retVal.init(null, null);
                                          }
                                      }
                                  });
                              }
                          }
                      });
            return retVal;
        }

        @Override
        public <A extends Collection<? super T>> MongoFuture<A> into(final A target) {
            final SingleResultFuture<A> future = new SingleResultFuture<A>();
            forEach(new Block<T>() {
                @Override
                public void apply(final T t) {
                    target.add(t);
                }
            }).register(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final MongoException e) {
                    if (e != null) {
                        future.init(null, e);
                    } else {
                        future.init(target, null);
                    }
                }
            });
            return future;
        }

        @Override
        public <U> org.mongodb.async.MongoIterable<U> map(final Function<T, U> mapper) {
            return new MappingIterable<T, U>(this, mapper);
        }

        @Override
        @SuppressWarnings("unchecked")
        public MongoFuture<WriteResult> replace(final T replacement) {
            notNull("replacement", replacement);
            return execute(new ReplaceOperation<T>(getNamespace(), true, options.getWriteConcern(),
                                                   asList(new ReplaceRequest<T>(find.getFilter(), replacement)),
                                                   new DocumentCodec(), getCodec()));
        }
    }
}
