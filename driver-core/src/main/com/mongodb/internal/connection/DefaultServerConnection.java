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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.WriteConcernResult;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;

public class DefaultServerConnection extends AbstractReferenceCounted implements Connection, AsyncConnection {
    private static final Logger LOGGER = Loggers.getLogger("connection");
    private final InternalConnection wrapped;
    private final ProtocolExecutor protocolExecutor;
    private final ClusterConnectionMode clusterConnectionMode;

    public DefaultServerConnection(final InternalConnection wrapped, final ProtocolExecutor protocolExecutor,
                            final ClusterConnectionMode clusterConnectionMode) {
        this.wrapped = wrapped;
        this.protocolExecutor = protocolExecutor;
        this.clusterConnectionMode = clusterConnectionMode;
    }

    @Override
    public DefaultServerConnection retain() {
        super.retain();
        return this;
    }

    @Override
    public int release() {
        int count = super.release();
        if (count == 0) {
            wrapped.close();
        }
        return count;
    }

    @Override
    public ConnectionDescription getDescription() {
        return wrapped.getDescription();
    }

    @Override
    public WriteConcernResult insert(final MongoNamespace namespace, final boolean ordered, final InsertRequest insertRequest,
            final RequestContext requestContext) {
        return executeProtocol(new InsertProtocol(namespace, ordered, insertRequest, requestContext));
    }

    @Override
    public void insertAsync(final MongoNamespace namespace, final boolean ordered, final InsertRequest insertRequest,
                            final RequestContext requestContext, final SingleResultCallback<WriteConcernResult> callback) {
        executeProtocolAsync(new InsertProtocol(namespace, ordered, insertRequest, requestContext), callback);
    }

    @Override
    public WriteConcernResult update(final MongoNamespace namespace, final boolean ordered, final UpdateRequest updateRequest,
            final RequestContext requestContext) {
        return executeProtocol(new UpdateProtocol(namespace, ordered, updateRequest, requestContext));
    }

    @Override
    public void updateAsync(final MongoNamespace namespace, final boolean ordered, final UpdateRequest updateRequest,
                            final RequestContext requestContext, final SingleResultCallback<WriteConcernResult> callback) {
        executeProtocolAsync(new UpdateProtocol(namespace, ordered, updateRequest, requestContext), callback);
    }

    @Override
    public WriteConcernResult delete(final MongoNamespace namespace, final boolean ordered, final DeleteRequest deleteRequest,
            final RequestContext requestContext) {
        return executeProtocol(new DeleteProtocol(namespace, ordered, deleteRequest, requestContext));
    }

    @Override
    public void deleteAsync(final MongoNamespace namespace, final boolean ordered, final DeleteRequest deleteRequest,
                            final RequestContext requestContext, final SingleResultCallback<WriteConcernResult> callback) {
        executeProtocolAsync(new DeleteProtocol(namespace, ordered, deleteRequest, requestContext), callback);
    }

    @Override
    public <T> T command(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
            final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
            @Nullable final ServerApi serverApi, final RequestContext requestContext) {
        return command(database, command, fieldNameValidator, readPreference, commandResultDecoder, sessionContext, serverApi,
                requestContext, true, null, null);
    }

    @Override
    public <T> T command(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
            final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
            @Nullable final ServerApi serverApi,
            final RequestContext requestContext, final boolean responseExpected, final SplittablePayload payload,
            final FieldNameValidator payloadFieldNameValidator) {
        return executeProtocol(new CommandProtocolImpl<T>(database, command, commandFieldNameValidator, readPreference,
                commandResultDecoder, responseExpected, payload, payloadFieldNameValidator, clusterConnectionMode, serverApi,
                        requestContext),
                sessionContext);
    }

    @Override
    public <T> void commandAsync(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                                 final ReadPreference readPreference, final Decoder<T> commandResultDecoder,
                                 final SessionContext sessionContext, final ServerApi serverApi, final RequestContext requestContext,
                                 final SingleResultCallback<T> callback) {
        commandAsync(database, command, fieldNameValidator, readPreference, commandResultDecoder, sessionContext, serverApi, requestContext,
                true, null, null, callback);
    }

    @Override
    public <T> void commandAsync(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                                 final ReadPreference readPreference, final Decoder<T> commandResultDecoder,
                                 final SessionContext sessionContext, final ServerApi serverApi, final RequestContext requestContext,
                                 final boolean responseExpected,
                                 final SplittablePayload payload, final FieldNameValidator payloadFieldNameValidator,
                                 final SingleResultCallback<T> callback) {
        executeProtocolAsync(new CommandProtocolImpl<T>(database, command, commandFieldNameValidator, readPreference,
                commandResultDecoder, responseExpected, payload,  payloadFieldNameValidator, clusterConnectionMode, serverApi,
                        requestContext),
                sessionContext, callback);
    }

    @Override
    public void markAsPinned(final PinningMode pinningMode) {
        wrapped.markAsPinned(pinningMode);
    }

    private <T> T executeProtocol(final LegacyProtocol<T> protocol) {
        return protocolExecutor.execute(protocol, this.wrapped);
    }

    private <T> T executeProtocol(final CommandProtocol<T> protocol, final SessionContext sessionContext) {
        return protocolExecutor.execute(protocol, this.wrapped, sessionContext);
    }

    private <T> void executeProtocolAsync(final LegacyProtocol<T> protocol, final SingleResultCallback<T> callback) {
        SingleResultCallback<T> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        try {
            protocolExecutor.executeAsync(protocol, this.wrapped, errHandlingCallback);
        } catch (Throwable t) {
            errHandlingCallback.onResult(null, t);
        }
    }

    private <T> void executeProtocolAsync(final CommandProtocol<T> protocol, final SessionContext sessionContext,
                                          final SingleResultCallback<T> callback) {
        SingleResultCallback<T> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        try {
            protocolExecutor.executeAsync(protocol, this.wrapped, sessionContext, errHandlingCallback);
        } catch (Throwable t) {
            errHandlingCallback.onResult(null, t);
        }
    }
}
