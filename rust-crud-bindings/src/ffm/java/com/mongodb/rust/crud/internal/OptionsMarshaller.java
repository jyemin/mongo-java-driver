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

package com.mongodb.rust.crud.internal;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TagSet;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.AggregateOptions;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ChangeStreamOptions;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DistinctOptions;
import com.mongodb.client.model.DropCollectionOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.TimeSeriesOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.ListCollectionsOptions;
import com.mongodb.client.model.ListDatabasesOptions;
import com.mongodb.client.model.ListIndexesOptions;
import com.mongodb.internal.rust.crud.ffi.BsonBatch;
import com.mongodb.internal.rust.crud.ffi.OperationContext;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import com.mongodb.session.ClientSession;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Marshals Java options objects to FFI option structs.
 */
public final class OptionsMarshaller {

    private OptionsMarshaller() {
    }

    /**
     * Converts a Collation to a BSON struct pointer, or NULL if collation is null.
     */
    private static MemorySegment toCollationBson(Arena arena, @Nullable Collation collation) {
        if (collation == null) {
            return MemorySegment.NULL;
        }
        return BsonMarshaller.toBsonStruct(arena, collation.asDocument());
    }

    /**
     * Converts a Bson to a BSON struct pointer, or NULL if null.
     */
    private static MemorySegment toBsonOrNull(Arena arena, @Nullable Bson bson) {
        if (bson == null) {
            return MemorySegment.NULL;
        }
        return BsonMarshaller.toBsonStruct(arena, bson);
    }

    /**
     * Converts a Boolean to a tri-state byte: -1 (unset), 0 (false), 1 (true).
     */
    private static byte toBooleanByte(@Nullable Boolean value) {
        if (value == null) {
            return (byte) -1;
        }
        return value ? (byte) 1 : (byte) 0;
    }

    /**
     * Converts a boolean to a byte: 0 (false), 1 (true).
     */
    private static byte toBooleanByte(boolean value) {
        return value ? (byte) 1 : (byte) 0;
    }

    /**
     * Converts ReturnDocument enum to FFI byte: 0 = BEFORE, 1 = AFTER.
     */
    private static byte toReturnDocumentByte(@Nullable ReturnDocument returnDocument) {
        if (returnDocument == null || returnDocument == ReturnDocument.BEFORE) {
            return (byte) 0;
        }
        return (byte) 1; // AFTER
    }

    /**
     * Converts FullDocument enum to FFI byte.
     * FFI values: 0=Default, 1=UpdateLookup, 2=WhenAvailable, 3=Required
     */
    private static byte toFullDocumentByte(@Nullable FullDocument fullDocument) {
        if (fullDocument == null) {
            return (byte) 0; // Default
        }
        switch (fullDocument) {
            case DEFAULT: return (byte) 0;
            case UPDATE_LOOKUP: return (byte) 1;
            case WHEN_AVAILABLE: return (byte) 2;
            case REQUIRED: return (byte) 3;
            default: return (byte) 0;
        }
    }

    /**
     * Converts FullDocumentBeforeChange enum to FFI byte.
     * FFI values: 0=Default, 1=Off, 2=WhenAvailable, 3=Required
     */
    private static byte toFullDocumentBeforeChangeByte(@Nullable FullDocumentBeforeChange fullDocumentBeforeChange) {
        if (fullDocumentBeforeChange == null) {
            return (byte) 0; // Default
        }
        switch (fullDocumentBeforeChange) {
            case DEFAULT: return (byte) 0;
            case OFF: return (byte) 1;
            case WHEN_AVAILABLE: return (byte) 2;
            case REQUIRED: return (byte) 3;
            default: return (byte) 0;
        }
    }

    /**
     * Converts hint (Bson or String) to BsonValue struct pointer.
     * Hint can be either a document (index spec) or a string (index name).
     */
    private static MemorySegment toHintBsonValue(Arena arena, @Nullable Bson hint, @Nullable String hintString) {
        if (hint != null) {
            return BsonMarshaller.toBsonValueStruct(arena, hint.toBsonDocument());
        } else if (hintString != null) {
            return BsonMarshaller.toBsonValueStruct(arena, new org.bson.BsonString(hintString));
        }
        return MemorySegment.NULL;
    }

    /**
     * Converts comment BsonValue to BsonValue struct pointer.
     */
    private static MemorySegment toCommentBsonValue(Arena arena, @Nullable org.bson.BsonValue comment) {
        if (comment != null) {
            return BsonMarshaller.toBsonValueStruct(arena, comment);
        }
        return MemorySegment.NULL;
    }

    // ==================== Concern Conversion Methods ====================

    /**
     * Creates ReadConcernOptions struct from Java ReadConcern.
     */
    public static MemorySegment toReadConcernOptions(Arena arena, @Nullable ReadConcern readConcern) {
        if (readConcern == null || readConcern.getLevel() == null) {
            return MemorySegment.NULL;
        }
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.ReadConcernOptions.allocate(arena);
        MemorySegment levelStr = arena.allocateFrom(readConcern.getLevel().getValue());
        com.mongodb.internal.rust.crud.ffi.ReadConcernOptions.level(opts, levelStr);
        return opts;
    }

    /**
     * Creates WriteConcernOptions struct from Java WriteConcern.
     */
    public static MemorySegment toWriteConcernOptions(Arena arena, @Nullable WriteConcern writeConcern) {
        if (writeConcern == null) {
            return MemorySegment.NULL;
        }
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.WriteConcernOptions.allocate(arena);

        // w can be an integer or a string (like "majority")
        String wString = writeConcern.getWString();
        if (wString != null) {
            // String-based w (e.g., "majority")
            com.mongodb.internal.rust.crud.ffi.WriteConcernOptions.w(opts, -1); // sentinel for "use w_tag"
            com.mongodb.internal.rust.crud.ffi.WriteConcernOptions.w_tag(opts, arena.allocateFrom(wString));
        } else {
            // Integer-based w
            int w = writeConcern.getW();
            com.mongodb.internal.rust.crud.ffi.WriteConcernOptions.w(opts, w);
            com.mongodb.internal.rust.crud.ffi.WriteConcernOptions.w_tag(opts, MemorySegment.NULL);
        }

        // journal
        Boolean journal = writeConcern.getJournal();
        com.mongodb.internal.rust.crud.ffi.WriteConcernOptions.journal(opts, toBooleanByte(journal));

        // w_timeout_ms
        Integer wTimeoutMs = writeConcern.getWTimeout(java.util.concurrent.TimeUnit.MILLISECONDS);
        com.mongodb.internal.rust.crud.ffi.WriteConcernOptions.w_timeout_ms(opts,
            wTimeoutMs != null ? wTimeoutMs.longValue() : -1L);

        return opts;
    }

    /**
     * Creates ReadPreferenceOptions struct from Java ReadPreference.
     */
    public static MemorySegment toReadPreferenceOptions(Arena arena, @Nullable ReadPreference readPreference) {
        if (readPreference == null) {
            return MemorySegment.NULL;
        }
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.ReadPreferenceOptions.allocate(arena);

        // mode: 0=Primary, 1=PrimaryPreferred, 2=Secondary, 3=SecondaryPreferred, 4=Nearest
        byte mode = toReadPreferenceMode(readPreference.getName());
        com.mongodb.internal.rust.crud.ffi.ReadPreferenceOptions.mode(opts, mode);

        // tags - convert TagSet list to BSON array
        if (readPreference instanceof com.mongodb.TaggableReadPreference) {
            com.mongodb.TaggableReadPreference taggable = (com.mongodb.TaggableReadPreference) readPreference;
            java.util.List<TagSet> tagSets = taggable.getTagSetList();
            if (tagSets != null && !tagSets.isEmpty()) {
                org.bson.BsonArray tagsArray = new org.bson.BsonArray();
                for (TagSet tagSet : tagSets) {
                    org.bson.BsonDocument tagDoc = new org.bson.BsonDocument();
                    for (com.mongodb.Tag tag : tagSet) {
                        tagDoc.put(tag.getName(), new org.bson.BsonString(tag.getValue()));
                    }
                    tagsArray.add(tagDoc);
                }
                org.bson.BsonDocument tagsDoc = new org.bson.BsonDocument("tags", tagsArray);
                com.mongodb.internal.rust.crud.ffi.ReadPreferenceOptions.tags(opts,
                    BsonMarshaller.toBsonStruct(arena, tagsDoc));
            } else {
                com.mongodb.internal.rust.crud.ffi.ReadPreferenceOptions.tags(opts, MemorySegment.NULL);
            }

            // max_staleness_seconds
            Long maxStaleness = taggable.getMaxStaleness(java.util.concurrent.TimeUnit.SECONDS);
            com.mongodb.internal.rust.crud.ffi.ReadPreferenceOptions.max_staleness_seconds(opts,
                maxStaleness != null ? maxStaleness : -1L);

            // hedge
            // TODO: HedgeOptions support if needed
            com.mongodb.internal.rust.crud.ffi.ReadPreferenceOptions.hedge(opts, MemorySegment.NULL);
        } else {
            com.mongodb.internal.rust.crud.ffi.ReadPreferenceOptions.tags(opts, MemorySegment.NULL);
            com.mongodb.internal.rust.crud.ffi.ReadPreferenceOptions.max_staleness_seconds(opts, -1L);
            com.mongodb.internal.rust.crud.ffi.ReadPreferenceOptions.hedge(opts, MemorySegment.NULL);
        }

        return opts;
    }

    /**
     * Converts ReadPreference name to FFI mode byte.
     */
    private static byte toReadPreferenceMode(String name) {
        switch (name.toLowerCase()) {
            case "primary": return (byte) 0;
            case "primarypreferred": return (byte) 1;
            case "secondary": return (byte) 2;
            case "secondarypreferred": return (byte) 3;
            case "nearest": return (byte) 4;
            default: return (byte) 0; // default to primary
        }
    }

    /**
     * Creates TransactionOptions struct from Java TransactionOptions.
     */
    public static MemorySegment toTransactionOptions(Arena arena, @Nullable com.mongodb.TransactionOptions txnOptions) {
        if (txnOptions == null) {
            return MemorySegment.NULL;
        }
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.TransactionOptions.allocate(arena);

        // Read concern level
        com.mongodb.ReadConcern rc = txnOptions.getReadConcern();
        if (rc != null && rc.getLevel() != null) {
            com.mongodb.internal.rust.crud.ffi.TransactionOptions.read_concern_level(opts,
                arena.allocateFrom(rc.getLevel().getValue()));
        } else {
            com.mongodb.internal.rust.crud.ffi.TransactionOptions.read_concern_level(opts, MemorySegment.NULL);
        }

        // Write concern
        com.mongodb.WriteConcern wc = txnOptions.getWriteConcern();
        if (wc != null) {
            String wString = wc.getWString();
            if (wString != null) {
                com.mongodb.internal.rust.crud.ffi.TransactionOptions.write_concern_w(opts, -1);
                com.mongodb.internal.rust.crud.ffi.TransactionOptions.write_concern_w_tag(opts,
                    arena.allocateFrom(wString));
            } else {
                com.mongodb.internal.rust.crud.ffi.TransactionOptions.write_concern_w(opts, wc.getW());
                com.mongodb.internal.rust.crud.ffi.TransactionOptions.write_concern_w_tag(opts, MemorySegment.NULL);
            }
            Boolean journal = wc.getJournal();
            com.mongodb.internal.rust.crud.ffi.TransactionOptions.write_concern_j(opts, toBooleanByte(journal));
            Integer wTimeout = wc.getWTimeout(java.util.concurrent.TimeUnit.MILLISECONDS);
            com.mongodb.internal.rust.crud.ffi.TransactionOptions.write_concern_w_timeout_ms(opts,
                wTimeout != null ? wTimeout.longValue() : -1L);
        } else {
            com.mongodb.internal.rust.crud.ffi.TransactionOptions.write_concern_w(opts, -1);
            com.mongodb.internal.rust.crud.ffi.TransactionOptions.write_concern_w_tag(opts, MemorySegment.NULL);
            com.mongodb.internal.rust.crud.ffi.TransactionOptions.write_concern_j(opts, (byte) -1);
            com.mongodb.internal.rust.crud.ffi.TransactionOptions.write_concern_w_timeout_ms(opts, -1L);
        }

        // Read preference mode
        com.mongodb.ReadPreference rp = txnOptions.getReadPreference();
        if (rp != null) {
            com.mongodb.internal.rust.crud.ffi.TransactionOptions.read_preference_mode(opts,
                toReadPreferenceMode(rp.getName()));
        } else {
            com.mongodb.internal.rust.crud.ffi.TransactionOptions.read_preference_mode(opts, (byte) 0);
        }

        // Max commit time
        Long maxCommitTime = txnOptions.getMaxCommitTime(java.util.concurrent.TimeUnit.MILLISECONDS);
        com.mongodb.internal.rust.crud.ffi.TransactionOptions.max_commit_time_ms(opts,
            maxCommitTime != null ? maxCommitTime : -1L);

        return opts;
    }

    /**
     * Creates SessionOptions struct from Java ClientSessionOptions.
     */
    public static MemorySegment toSessionOptions(Arena arena, @Nullable com.mongodb.ClientSessionOptions sessionOptions) {
        if (sessionOptions == null) {
            return MemorySegment.NULL;
        }
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.SessionOptions.allocate(arena);

        // Causal consistency
        Boolean cc = sessionOptions.isCausallyConsistent();
        com.mongodb.internal.rust.crud.ffi.SessionOptions.causal_consistency(opts, toBooleanByte(cc));

        // Snapshot
        Boolean snapshot = sessionOptions.isSnapshot();
        com.mongodb.internal.rust.crud.ffi.SessionOptions.snapshot(opts, toBooleanByte(snapshot));

        // Default transaction options
        com.mongodb.TransactionOptions defaultTxnOpts = sessionOptions.getDefaultTransactionOptions();
        if (defaultTxnOpts != null) {
            MemorySegment txnOpts = toTransactionOptions(arena, defaultTxnOpts);
            com.mongodb.internal.rust.crud.ffi.SessionOptions.default_transaction_options(opts, txnOpts);
        } else {
            com.mongodb.internal.rust.crud.ffi.SessionOptions.default_transaction_options(opts, MemorySegment.NULL);
        }

        return opts;
    }

    // ==================== OperationContext ====================

    /**
     * Creates an OperationContext struct with default settings.
     */
    public static MemorySegment createOperationContext(Arena arena, @Nullable ClientSession session) {
        return createOperationContext(arena, session, null, null, null, -1L);
    }

    /**
     * Creates an OperationContext struct with full settings.
     *
     * @param session the client session (or null)
     * @param readPreference the read preference (or null for default)
     * @param writeConcern the write concern (or null for default)
     * @param readConcern the read concern (or null for default)
     * @param timeoutMs operation timeout in milliseconds (-1 for no timeout)
     */
    public static MemorySegment createOperationContext(
            Arena arena,
            @Nullable ClientSession session,
            @Nullable ReadPreference readPreference,
            @Nullable WriteConcern writeConcern,
            @Nullable ReadConcern readConcern,
            long timeoutMs) {
        MemorySegment ctx = OperationContext.allocate(arena);

        // Session - TODO: Map ClientSession to FFI Session pointer
        OperationContext.session(ctx, MemorySegment.NULL);

        // Read preference
        OperationContext.read_preference(ctx, toReadPreferenceOptions(arena, readPreference));

        // Write concern
        OperationContext.write_concern(ctx, toWriteConcernOptions(arena, writeConcern));

        // Read concern
        OperationContext.read_concern(ctx, toReadConcernOptions(arena, readConcern));

        // Timeout
        OperationContext.timeout_ms(ctx, timeoutMs);

        return ctx;
    }

    /**
     * Creates an OperationContext struct with a raw session pointer.
     * Used by FfmAsyncClient which extracts the pointer from NativeAsyncClientSession.
     */
    public static MemorySegment createOperationContextWithSessionPtr(Arena arena, MemorySegment sessionPtr) {
        MemorySegment ctx = OperationContext.allocate(arena);
        OperationContext.session(ctx, sessionPtr);
        OperationContext.read_preference(ctx, MemorySegment.NULL);
        OperationContext.write_concern(ctx, MemorySegment.NULL);
        OperationContext.read_concern(ctx, MemorySegment.NULL);
        OperationContext.timeout_ms(ctx, -1L);
        return ctx;
    }

    /**
     * Creates InsertOneOptions struct.
     */
    public static MemorySegment toInsertOneOptions(Arena arena, InsertOneOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.InsertOneOptions.allocate(arena);
        
        Boolean bypass = options.getBypassDocumentValidation();
        com.mongodb.internal.rust.crud.ffi.InsertOneOptions.bypass_document_validation(opts, 
            bypass == null ? (byte) -1 : (bypass ? (byte) 1 : (byte) 0));
        
        if (options.getComment() != null) {
            MemorySegment comment = BsonMarshaller.toBsonValueStruct(arena, options.getComment());
            com.mongodb.internal.rust.crud.ffi.InsertOneOptions.comment(opts, comment);
        } else {
            com.mongodb.internal.rust.crud.ffi.InsertOneOptions.comment(opts, MemorySegment.NULL);
        }
        
        return opts;
    }

    /**
     * Creates InsertManyOptions struct.
     */
    public static MemorySegment toInsertManyOptions(Arena arena, InsertManyOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.InsertManyOptions.allocate(arena);
        
        Boolean bypass = options.getBypassDocumentValidation();
        com.mongodb.internal.rust.crud.ffi.InsertManyOptions.bypass_document_validation(opts,
            bypass == null ? (byte) -1 : (bypass ? (byte) 1 : (byte) 0));
        
        Boolean ordered = options.isOrdered();
        com.mongodb.internal.rust.crud.ffi.InsertManyOptions.ordered(opts,
            ordered ? (byte) 1 : (byte) 0);
        
        if (options.getComment() != null) {
            MemorySegment comment = BsonMarshaller.toBsonValueStruct(arena, options.getComment());
            com.mongodb.internal.rust.crud.ffi.InsertManyOptions.comment(opts, comment);
        } else {
            com.mongodb.internal.rust.crud.ffi.InsertManyOptions.comment(opts, MemorySegment.NULL);
        }
        
        return opts;
    }

    /**
     * Creates UpdateOptions struct.
     */
    public static MemorySegment toUpdateOptions(Arena arena, UpdateOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.UpdateOptions.allocate(arena);
        
        com.mongodb.internal.rust.crud.ffi.UpdateOptions.upsert(opts,
            options.isUpsert() ? (byte) 1 : (byte) 0);
        
        Boolean bypass = options.getBypassDocumentValidation();
        com.mongodb.internal.rust.crud.ffi.UpdateOptions.bypass_document_validation(opts,
            bypass == null ? (byte) -1 : (bypass ? (byte) 1 : (byte) 0));
        
        // Array filters
        if (options.getArrayFilters() != null) {
            BsonDocument arrayFiltersDoc = new BsonDocument();
            int i = 0;
            for (var filter : options.getArrayFilters()) {
                arrayFiltersDoc.put(String.valueOf(i++), filter.toBsonDocument());
            }
            com.mongodb.internal.rust.crud.ffi.UpdateOptions.array_filters(opts, 
                BsonMarshaller.toBsonStruct(arena, arrayFiltersDoc));
        } else {
            com.mongodb.internal.rust.crud.ffi.UpdateOptions.array_filters(opts, MemorySegment.NULL);
        }
        
        // Hint
        if (options.getHint() != null) {
            com.mongodb.internal.rust.crud.ffi.UpdateOptions.hint(opts,
                BsonMarshaller.toBsonValueStruct(arena, options.getHint().toBsonDocument()));
        } else if (options.getHintString() != null) {
            com.mongodb.internal.rust.crud.ffi.UpdateOptions.hint(opts,
                BsonMarshaller.toBsonValueStruct(arena, new org.bson.BsonString(options.getHintString())));
        } else {
            com.mongodb.internal.rust.crud.ffi.UpdateOptions.hint(opts, MemorySegment.NULL);
        }
        
        // Collation
        com.mongodb.internal.rust.crud.ffi.UpdateOptions.collation(opts, toCollationBson(arena, options.getCollation()));
        
        // Comment
        if (options.getComment() != null) {
            com.mongodb.internal.rust.crud.ffi.UpdateOptions.comment(opts,
                BsonMarshaller.toBsonValueStruct(arena, options.getComment()));
        } else {
            com.mongodb.internal.rust.crud.ffi.UpdateOptions.comment(opts, MemorySegment.NULL);
        }

        return opts;
    }

    /**
     * Creates ReplaceOptions struct.
     */
    public static MemorySegment toReplaceOptions(Arena arena, ReplaceOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.ReplaceOptions.allocate(arena);

        com.mongodb.internal.rust.crud.ffi.ReplaceOptions.upsert(opts,
            options.isUpsert() ? (byte) 1 : (byte) 0);

        Boolean bypass = options.getBypassDocumentValidation();
        com.mongodb.internal.rust.crud.ffi.ReplaceOptions.bypass_document_validation(opts,
            bypass == null ? (byte) -1 : (bypass ? (byte) 1 : (byte) 0));

        // Hint, collation, comment
        com.mongodb.internal.rust.crud.ffi.ReplaceOptions.hint(opts, toHintBsonValue(arena, options.getHint(), options.getHintString()));
        com.mongodb.internal.rust.crud.ffi.ReplaceOptions.collation(opts, toCollationBson(arena, options.getCollation()));
        com.mongodb.internal.rust.crud.ffi.ReplaceOptions.comment(opts, toCommentBsonValue(arena, options.getComment()));

        return opts;
    }

    /**
     * Creates DeleteOptions struct.
     */
    public static MemorySegment toDeleteOptions(Arena arena, DeleteOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.DeleteOptions.allocate(arena);

        // Collation, hint, comment
        com.mongodb.internal.rust.crud.ffi.DeleteOptions.collation(opts, toCollationBson(arena, options.getCollation()));
        com.mongodb.internal.rust.crud.ffi.DeleteOptions.hint(opts, toHintBsonValue(arena, options.getHint(), options.getHintString()));
        com.mongodb.internal.rust.crud.ffi.DeleteOptions.comment(opts, toCommentBsonValue(arena, options.getComment()));

        return opts;
    }

    /**
     * Creates FindOneOptions struct.
     */
    public static MemorySegment toFindOneOptions(Arena arena, FindOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.FindOneOptions.allocate(arena);

        if (options.getProjection() != null) {
            com.mongodb.internal.rust.crud.ffi.FindOneOptions.projection(opts,
                BsonMarshaller.toBsonStruct(arena, options.getProjection()));
        } else {
            com.mongodb.internal.rust.crud.ffi.FindOneOptions.projection(opts, MemorySegment.NULL);
        }

        if (options.getSort() != null) {
            com.mongodb.internal.rust.crud.ffi.FindOneOptions.sort(opts,
                BsonMarshaller.toBsonStruct(arena, options.getSort()));
        } else {
            com.mongodb.internal.rust.crud.ffi.FindOneOptions.sort(opts, MemorySegment.NULL);
        }

        com.mongodb.internal.rust.crud.ffi.FindOneOptions.skip(opts, options.getSkip());
        com.mongodb.internal.rust.crud.ffi.FindOneOptions.hint(opts, toHintBsonValue(arena, options.getHint(), options.getHintString()));
        com.mongodb.internal.rust.crud.ffi.FindOneOptions.collation(opts, toCollationBson(arena, options.getCollation()));
        com.mongodb.internal.rust.crud.ffi.FindOneOptions.comment(opts, toCommentBsonValue(arena, options.getComment()));

        return opts;
    }

    /**
     * Creates FindOptions struct for find with cursor.
     */
    public static MemorySegment toFindOptions(Arena arena, FindOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.FindOptions.allocate(arena);

        if (options.getProjection() != null) {
            com.mongodb.internal.rust.crud.ffi.FindOptions.projection(opts,
                BsonMarshaller.toBsonStruct(arena, options.getProjection()));
        } else {
            com.mongodb.internal.rust.crud.ffi.FindOptions.projection(opts, MemorySegment.NULL);
        }

        if (options.getSort() != null) {
            com.mongodb.internal.rust.crud.ffi.FindOptions.sort(opts,
                BsonMarshaller.toBsonStruct(arena, options.getSort()));
        } else {
            com.mongodb.internal.rust.crud.ffi.FindOptions.sort(opts, MemorySegment.NULL);
        }

        com.mongodb.internal.rust.crud.ffi.FindOptions.limit(opts, options.getLimit());
        com.mongodb.internal.rust.crud.ffi.FindOptions.skip(opts, options.getSkip());
        com.mongodb.internal.rust.crud.ffi.FindOptions.batch_size(opts, options.getBatchSize());
        com.mongodb.internal.rust.crud.ffi.FindOptions.hint(opts, toHintBsonValue(arena, options.getHint(), options.getHintString()));
        com.mongodb.internal.rust.crud.ffi.FindOptions.collation(opts, toCollationBson(arena, options.getCollation()));
        com.mongodb.internal.rust.crud.ffi.FindOptions.comment(opts, toCommentBsonValue(arena, options.getComment()));

        return opts;
    }

    /**
     * Creates FindOneAndUpdateOptions struct.
     */
    public static MemorySegment toFindOneAndUpdateOptions(Arena arena, FindOneAndUpdateOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.FindOneAndUpdateOptions.allocate(arena);

        // Projection and sort
        com.mongodb.internal.rust.crud.ffi.FindOneAndUpdateOptions.projection(opts, toBsonOrNull(arena, options.getProjection()));
        com.mongodb.internal.rust.crud.ffi.FindOneAndUpdateOptions.sort(opts, toBsonOrNull(arena, options.getSort()));

        // Upsert and return document
        com.mongodb.internal.rust.crud.ffi.FindOneAndUpdateOptions.upsert(opts, toBooleanByte(options.isUpsert()));
        com.mongodb.internal.rust.crud.ffi.FindOneAndUpdateOptions.return_document(opts, toReturnDocumentByte(options.getReturnDocument()));

        // Bypass document validation
        com.mongodb.internal.rust.crud.ffi.FindOneAndUpdateOptions.bypass_document_validation(opts, toBooleanByte(options.getBypassDocumentValidation()));

        // Array filters - TODO: implement array filters marshalling
        com.mongodb.internal.rust.crud.ffi.FindOneAndUpdateOptions.array_filters(opts, MemorySegment.NULL);

        // Hint, collation, comment
        com.mongodb.internal.rust.crud.ffi.FindOneAndUpdateOptions.hint(opts, toHintBsonValue(arena, options.getHint(), options.getHintString()));
        com.mongodb.internal.rust.crud.ffi.FindOneAndUpdateOptions.collation(opts, toCollationBson(arena, options.getCollation()));
        com.mongodb.internal.rust.crud.ffi.FindOneAndUpdateOptions.comment(opts, toCommentBsonValue(arena, options.getComment()));

        return opts;
    }

    /**
     * Creates FindOneAndReplaceOptions struct.
     */
    public static MemorySegment toFindOneAndReplaceOptions(Arena arena, FindOneAndReplaceOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.FindOneAndReplaceOptions.allocate(arena);

        // Projection and sort
        com.mongodb.internal.rust.crud.ffi.FindOneAndReplaceOptions.projection(opts, toBsonOrNull(arena, options.getProjection()));
        com.mongodb.internal.rust.crud.ffi.FindOneAndReplaceOptions.sort(opts, toBsonOrNull(arena, options.getSort()));

        // Upsert and return document
        com.mongodb.internal.rust.crud.ffi.FindOneAndReplaceOptions.upsert(opts, toBooleanByte(options.isUpsert()));
        com.mongodb.internal.rust.crud.ffi.FindOneAndReplaceOptions.return_document(opts, toReturnDocumentByte(options.getReturnDocument()));

        // Bypass document validation
        com.mongodb.internal.rust.crud.ffi.FindOneAndReplaceOptions.bypass_document_validation(opts, toBooleanByte(options.getBypassDocumentValidation()));

        // Hint, collation, comment
        com.mongodb.internal.rust.crud.ffi.FindOneAndReplaceOptions.hint(opts, toHintBsonValue(arena, options.getHint(), options.getHintString()));
        com.mongodb.internal.rust.crud.ffi.FindOneAndReplaceOptions.collation(opts, toCollationBson(arena, options.getCollation()));
        com.mongodb.internal.rust.crud.ffi.FindOneAndReplaceOptions.comment(opts, toCommentBsonValue(arena, options.getComment()));

        return opts;
    }

    /**
     * Creates FindOneAndDeleteOptions struct.
     */
    public static MemorySegment toFindOneAndDeleteOptions(Arena arena, FindOneAndDeleteOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.FindOneAndDeleteOptions.allocate(arena);

        // Projection and sort
        com.mongodb.internal.rust.crud.ffi.FindOneAndDeleteOptions.projection(opts, toBsonOrNull(arena, options.getProjection()));
        com.mongodb.internal.rust.crud.ffi.FindOneAndDeleteOptions.sort(opts, toBsonOrNull(arena, options.getSort()));

        // Hint, collation, comment
        com.mongodb.internal.rust.crud.ffi.FindOneAndDeleteOptions.hint(opts, toHintBsonValue(arena, options.getHint(), options.getHintString()));
        com.mongodb.internal.rust.crud.ffi.FindOneAndDeleteOptions.collation(opts, toCollationBson(arena, options.getCollation()));
        com.mongodb.internal.rust.crud.ffi.FindOneAndDeleteOptions.comment(opts, toCommentBsonValue(arena, options.getComment()));

        return opts;
    }

    /**
     * Creates a BsonBatch from a list of documents.
     */
    public static MemorySegment toBsonBatch(Arena arena, List<BsonDocument> documents) {
        if (documents.isEmpty()) {
            MemorySegment batch = BsonBatch.allocate(arena);
            BsonBatch.data(batch, MemorySegment.NULL);
            BsonBatch.len(batch, 0);
            BsonBatch.offsets(batch, MemorySegment.NULL);
            BsonBatch.count(batch, 0);
            return batch;
        }

        // Encode all documents and compute offsets
        byte[][] encodedDocs = new byte[documents.size()][];
        int totalLen = 0;
        for (int i = 0; i < documents.size(); i++) {
            encodedDocs[i] = BsonMarshaller.encode(documents.get(i));
            totalLen += encodedDocs[i].length;
        }

        // Allocate data buffer
        MemorySegment dataSegment = arena.allocate(totalLen);
        MemorySegment offsetsSegment = arena.allocate((long) documents.size() * Integer.BYTES);

        int offset = 0;
        for (int i = 0; i < encodedDocs.length; i++) {
            offsetsSegment.setAtIndex(java.lang.foreign.ValueLayout.JAVA_INT, i, offset);
            dataSegment.asSlice(offset, encodedDocs[i].length)
                .copyFrom(MemorySegment.ofArray(encodedDocs[i]));
            offset += encodedDocs[i].length;
        }

        MemorySegment batch = BsonBatch.allocate(arena);
        BsonBatch.data(batch, dataSegment);
        BsonBatch.len(batch, totalLen);
        BsonBatch.offsets(batch, offsetsSegment);
        BsonBatch.count(batch, documents.size());

        return batch;
    }

    // ==================== Additional Options Marshallers ====================

    /**
     * Creates AggregateOptions struct.
     *
     * @param bypassDocumentValidation separate parameter since AggregateOptions class doesn't have it
     */
    public static MemorySegment toAggregateOptions(Arena arena, AggregateOptions options,
            @Nullable Boolean bypassDocumentValidation) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.AggregateOptions.allocate(arena);

        Boolean allowDiskUse = options.getAllowDiskUse();
        com.mongodb.internal.rust.crud.ffi.AggregateOptions.allow_disk_use(opts,
            allowDiskUse == null ? (byte) -1 : (allowDiskUse ? (byte) 1 : (byte) 0));

        com.mongodb.internal.rust.crud.ffi.AggregateOptions.batch_size(opts, options.getBatchSize());
        com.mongodb.internal.rust.crud.ffi.AggregateOptions.bypass_document_validation(opts,
            toBooleanByte(bypassDocumentValidation));
        com.mongodb.internal.rust.crud.ffi.AggregateOptions.collation(opts, toCollationBson(arena, options.getCollation()));
        com.mongodb.internal.rust.crud.ffi.AggregateOptions.hint(opts, toHintBsonValue(arena, options.getHint(), options.getHintString()));
        com.mongodb.internal.rust.crud.ffi.AggregateOptions.comment(opts, toCommentBsonValue(arena, options.getComment()));
        com.mongodb.internal.rust.crud.ffi.AggregateOptions.let_vars(opts, toBsonOrNull(arena, options.getLet()));

        return opts;
    }

    /**
     * Creates CountOptions struct.
     */
    public static MemorySegment toCountOptions(Arena arena, CountOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.CountOptions.allocate(arena);

        // getSkip/getLimit return int primitives; 0 is the default meaning "not set"
        int skip = options.getSkip();
        int limit = options.getLimit();
        com.mongodb.internal.rust.crud.ffi.CountOptions.skip(opts, skip > 0 ? skip : -1);
        com.mongodb.internal.rust.crud.ffi.CountOptions.limit(opts, limit > 0 ? limit : -1);
        com.mongodb.internal.rust.crud.ffi.CountOptions.collation(opts, toCollationBson(arena, options.getCollation()));
        com.mongodb.internal.rust.crud.ffi.CountOptions.hint(opts, toHintBsonValue(arena, options.getHint(), options.getHintString()));
        com.mongodb.internal.rust.crud.ffi.CountOptions.comment(opts, toCommentBsonValue(arena, options.getComment()));

        return opts;
    }

    /**
     * Creates EstimatedDocumentCountOptions struct.
     */
    public static MemorySegment toEstimatedDocumentCountOptions(Arena arena, EstimatedDocumentCountOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.EstimatedDocumentCountOptions.allocate(arena);
        com.mongodb.internal.rust.crud.ffi.EstimatedDocumentCountOptions.comment(opts, MemorySegment.NULL);
        return opts;
    }

    /**
     * Creates DistinctOptions struct.
     */
    public static MemorySegment toDistinctOptions(Arena arena, DistinctOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.DistinctOptions.allocate(arena);
        com.mongodb.internal.rust.crud.ffi.DistinctOptions.collation(opts, toCollationBson(arena, options.getCollation()));
        com.mongodb.internal.rust.crud.ffi.DistinctOptions.comment(opts, toCommentBsonValue(arena, options.getComment()));
        return opts;
    }

    /**
     * Creates CreateIndexOptions struct.
     */
    public static MemorySegment toCreateIndexOptions(Arena arena, CreateIndexOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.CreateIndexOptions.allocate(arena);
        com.mongodb.internal.rust.crud.ffi.CreateIndexOptions.comment(opts, MemorySegment.NULL);
        return opts;
    }

    /**
     * Creates DropIndexOptions struct.
     */
    public static MemorySegment toDropIndexOptions(Arena arena, DropIndexOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.DropIndexOptions.allocate(arena);
        com.mongodb.internal.rust.crud.ffi.DropIndexOptions.comment(opts, MemorySegment.NULL);
        return opts;
    }

    /**
     * Creates CreateCollectionOptions struct.
     */
    public static MemorySegment toCreateCollectionOptions(Arena arena, CreateCollectionOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.allocate(arena);

        // Capped collection options
        com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.capped(opts,
            toBooleanByte(options.isCapped()));
        com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.size(opts,
            options.getSizeInBytes() > 0 ? options.getSizeInBytes() : -1L);
        com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.max(opts,
            options.getMaxDocuments() > 0 ? options.getMaxDocuments() : -1L);

        // Storage engine
        com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.storage_engine(opts,
            toBsonOrNull(arena, options.getStorageEngineOptions()));

        // Validation options
        ValidationOptions validation = options.getValidationOptions();
        if (validation != null) {
            com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.validator(opts,
                toBsonOrNull(arena, validation.getValidator()));

            // validation_level as string
            if (validation.getValidationLevel() != null) {
                MemorySegment levelStr = arena.allocateFrom(validation.getValidationLevel().getValue());
                com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.validation_level(opts, levelStr);
            } else {
                com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.validation_level(opts, MemorySegment.NULL);
            }

            // validation_action as string
            if (validation.getValidationAction() != null) {
                MemorySegment actionStr = arena.allocateFrom(validation.getValidationAction().getValue());
                com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.validation_action(opts, actionStr);
            } else {
                com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.validation_action(opts, MemorySegment.NULL);
            }
        } else {
            com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.validator(opts, MemorySegment.NULL);
            com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.validation_level(opts, MemorySegment.NULL);
            com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.validation_action(opts, MemorySegment.NULL);
        }

        // Collation
        com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.collation(opts,
            toCollationBson(arena, options.getCollation()));

        // TTL (expire_after_seconds)
        long expireAfter = options.getExpireAfter(TimeUnit.SECONDS);
        com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.expire_after_seconds(opts,
            expireAfter > 0 ? expireAfter : -1L);

        // Time series options - convert to BSON document
        TimeSeriesOptions timeSeries = options.getTimeSeriesOptions();
        if (timeSeries != null) {
            BsonDocument tsDoc = new BsonDocument();
            tsDoc.put("timeField", new org.bson.BsonString(timeSeries.getTimeField()));
            if (timeSeries.getMetaField() != null) {
                tsDoc.put("metaField", new org.bson.BsonString(timeSeries.getMetaField()));
            }
            if (timeSeries.getGranularity() != null) {
                tsDoc.put("granularity", new org.bson.BsonString(timeSeries.getGranularity().name().toLowerCase()));
            }
            com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.timeseries(opts,
                BsonMarshaller.toBsonStruct(arena, tsDoc));
        } else {
            com.mongodb.internal.rust.crud.ffi.CreateCollectionOptions.timeseries(opts, MemorySegment.NULL);
        }

        return opts;
    }

    /**
     * Creates DropCollectionOptions struct.
     */
    public static MemorySegment toDropCollectionOptions(Arena arena, DropCollectionOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.DropCollectionOptions.allocate(arena);
        return opts;
    }

    /**
     * Creates ChangeStreamOptions struct.
     */
    public static MemorySegment toChangeStreamOptions(Arena arena, ChangeStreamOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.ChangeStreamOptions.allocate(arena);

        com.mongodb.internal.rust.crud.ffi.ChangeStreamOptions.batch_size(opts, options.getBatchSize());

        // Resume tokens - BsonDocument pointers
        BsonDocument resumeAfter = options.getResumeAfter();
        com.mongodb.internal.rust.crud.ffi.ChangeStreamOptions.resume_after(opts,
            resumeAfter != null ? BsonMarshaller.toBsonStruct(arena, resumeAfter) : MemorySegment.NULL);

        BsonDocument startAfter = options.getStartAfter();
        com.mongodb.internal.rust.crud.ffi.ChangeStreamOptions.start_after(opts,
            startAfter != null ? BsonMarshaller.toBsonStruct(arena, startAfter) : MemorySegment.NULL);

        // Start at operation time - BsonTimestamp converted to Bson document
        org.bson.BsonTimestamp startAt = options.getStartAtOperationTime();
        if (startAt != null) {
            BsonDocument startAtDoc = new BsonDocument("ts", startAt);
            com.mongodb.internal.rust.crud.ffi.ChangeStreamOptions.start_at_operation_time(opts,
                BsonMarshaller.toBsonStruct(arena, startAtDoc));
        } else {
            com.mongodb.internal.rust.crud.ffi.ChangeStreamOptions.start_at_operation_time(opts, MemorySegment.NULL);
        }

        // Full document options
        com.mongodb.internal.rust.crud.ffi.ChangeStreamOptions.full_document(opts,
            toFullDocumentByte(options.getFullDocument()));
        com.mongodb.internal.rust.crud.ffi.ChangeStreamOptions.full_document_before_change(opts,
            toFullDocumentBeforeChangeByte(options.getFullDocumentBeforeChange()));

        com.mongodb.internal.rust.crud.ffi.ChangeStreamOptions.collation(opts, toCollationBson(arena, options.getCollation()));
        com.mongodb.internal.rust.crud.ffi.ChangeStreamOptions.comment(opts, toCommentBsonValue(arena, options.getComment()));

        return opts;
    }

    /**
     * Creates BulkWriteOptions struct.
     */
    public static MemorySegment toBulkWriteOptions(Arena arena, BulkWriteOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.BulkWriteOptions.allocate(arena);

        com.mongodb.internal.rust.crud.ffi.BulkWriteOptions.ordered(opts,
            options.isOrdered() ? (byte) 1 : (byte) 0);

        Boolean bypass = options.getBypassDocumentValidation();
        com.mongodb.internal.rust.crud.ffi.BulkWriteOptions.bypass_document_validation(opts,
            bypass == null ? (byte) -1 : (bypass ? (byte) 1 : (byte) 0));

        com.mongodb.internal.rust.crud.ffi.BulkWriteOptions.comment(opts, toCommentBsonValue(arena, options.getComment()));
        com.mongodb.internal.rust.crud.ffi.BulkWriteOptions.let_vars(opts, MemorySegment.NULL);

        return opts;
    }

    /**
     * Creates an FFI IndexOptions struct from Java IndexOptions.
     */
    public static MemorySegment toIndexOptions(Arena arena, @Nullable IndexOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }

        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.IndexOptions.allocate(arena);

        // Name (const char *)
        String name = options.getName();
        if (name != null) {
            MemorySegment nameSegment = arena.allocateFrom(name);
            com.mongodb.internal.rust.crud.ffi.IndexOptions.name(opts, nameSegment);
        } else {
            com.mongodb.internal.rust.crud.ffi.IndexOptions.name(opts, MemorySegment.NULL);
        }

        // Boolean options as tri-state bytes
        com.mongodb.internal.rust.crud.ffi.IndexOptions.unique(opts, toBooleanByte(options.isUnique()));
        com.mongodb.internal.rust.crud.ffi.IndexOptions.sparse(opts, toBooleanByte(options.isSparse()));
        com.mongodb.internal.rust.crud.ffi.IndexOptions.background(opts, toBooleanByte(options.isBackground()));
        com.mongodb.internal.rust.crud.ffi.IndexOptions.hidden(opts, toBooleanByte(options.isHidden()));

        // TTL index expire_after_seconds
        Long expireAfter = options.getExpireAfter(TimeUnit.SECONDS);
        com.mongodb.internal.rust.crud.ffi.IndexOptions.expire_after_seconds(opts,
            expireAfter != null ? expireAfter : -1L);

        // Bson options
        com.mongodb.internal.rust.crud.ffi.IndexOptions.partial_filter_expression(opts,
            toBsonOrNull(arena, options.getPartialFilterExpression()));
        com.mongodb.internal.rust.crud.ffi.IndexOptions.collation(opts,
            toCollationBson(arena, options.getCollation()));
        com.mongodb.internal.rust.crud.ffi.IndexOptions.wildcard_projection(opts,
            toBsonOrNull(arena, options.getWildcardProjection()));

        return opts;
    }

    /**
     * Converts IndexModel list to an array of FFI IndexModel structs.
     * Returns a struct containing the array pointer and length.
     */
    public static IndexModelArray toIndexModelArray(Arena arena, List<IndexModel> indexes) {
        if (indexes.isEmpty()) {
            return new IndexModelArray(MemorySegment.NULL, 0);
        }

        long structSize = com.mongodb.internal.rust.crud.ffi.IndexModel.sizeof();
        MemorySegment array = arena.allocate(structSize * indexes.size());

        for (int i = 0; i < indexes.size(); i++) {
            IndexModel javaModel = indexes.get(i);
            MemorySegment ffiModel = array.asSlice(i * structSize, structSize);

            // Set keys
            MemorySegment keysBson = BsonMarshaller.toBsonStruct(arena, javaModel.getKeys());
            com.mongodb.internal.rust.crud.ffi.IndexModel.keys(ffiModel, keysBson);

            // Set options
            com.mongodb.internal.rust.crud.ffi.IndexModel.options(ffiModel,
                toIndexOptions(arena, javaModel.getOptions()));
        }

        return new IndexModelArray(array, indexes.size());
    }

    /**
     * Helper class to hold an array pointer and its length.
     */
    public static class IndexModelArray {
        public final MemorySegment array;
        public final long length;

        public IndexModelArray(MemorySegment array, long length) {
            this.array = array;
            this.length = length;
        }
    }

    // BulkWriteModel operation type constants (assumed based on typical enum ordering)
    private static final byte OP_INSERT_ONE = 0;
    private static final byte OP_UPDATE_ONE = 1;
    private static final byte OP_UPDATE_MANY = 2;
    private static final byte OP_REPLACE_ONE = 3;
    private static final byte OP_DELETE_ONE = 4;
    private static final byte OP_DELETE_MANY = 5;

    /**
     * Converts WriteModel list to an array of FFI BulkWriteModel structs.
     * Returns a struct containing the array pointer and length.
     */
    public static WriteModelArray toWriteModelArray(Arena arena, List<? extends WriteModel<BsonDocument>> models) {
        if (models.isEmpty()) {
            return new WriteModelArray(MemorySegment.NULL, 0);
        }

        long structSize = com.mongodb.internal.rust.crud.ffi.BulkWriteModel.sizeof();
        MemorySegment array = arena.allocate(structSize * models.size());

        for (int i = 0; i < models.size(); i++) {
            WriteModel<BsonDocument> model = models.get(i);
            MemorySegment ffiModel = array.asSlice(i * structSize, structSize);

            // Initialize with nulls
            com.mongodb.internal.rust.crud.ffi.BulkWriteModel.namespace_(ffiModel, MemorySegment.NULL);
            com.mongodb.internal.rust.crud.ffi.BulkWriteModel.document(ffiModel, MemorySegment.NULL);
            com.mongodb.internal.rust.crud.ffi.BulkWriteModel.update(ffiModel, MemorySegment.NULL);
            com.mongodb.internal.rust.crud.ffi.BulkWriteModel.array_filters(ffiModel, MemorySegment.NULL);
            com.mongodb.internal.rust.crud.ffi.BulkWriteModel.collation(ffiModel, MemorySegment.NULL);
            com.mongodb.internal.rust.crud.ffi.BulkWriteModel.hint(ffiModel, MemorySegment.NULL);
            com.mongodb.internal.rust.crud.ffi.BulkWriteModel.upsert(ffiModel, (byte) -1);

            if (model instanceof InsertOneModel) {
                InsertOneModel<BsonDocument> insert = (InsertOneModel<BsonDocument>) model;
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.operation(ffiModel, OP_INSERT_ONE);
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.document(ffiModel,
                    BsonMarshaller.toBsonStruct(arena, insert.getDocument()));
                // InsertOneModel has no collation/hint options
            } else if (model instanceof UpdateOneModel) {
                UpdateOneModel<BsonDocument> update = (UpdateOneModel<BsonDocument>) model;
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.operation(ffiModel, OP_UPDATE_ONE);
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.document(ffiModel,
                    BsonMarshaller.toBsonStruct(arena, update.getFilter()));
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.update(ffiModel,
                    BsonMarshaller.toBsonStruct(arena, update.getUpdate()));
                // Set options from UpdateOptions
                UpdateOptions opts = update.getOptions();
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.collation(ffiModel,
                    toCollationBson(arena, opts.getCollation()));
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.hint(ffiModel,
                    toHintBsonValue(arena, opts.getHint(), opts.getHintString()));
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.upsert(ffiModel,
                    toBooleanByte(opts.isUpsert()));
            } else if (model instanceof UpdateManyModel) {
                UpdateManyModel<BsonDocument> update = (UpdateManyModel<BsonDocument>) model;
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.operation(ffiModel, OP_UPDATE_MANY);
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.document(ffiModel,
                    BsonMarshaller.toBsonStruct(arena, update.getFilter()));
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.update(ffiModel,
                    BsonMarshaller.toBsonStruct(arena, update.getUpdate()));
                // Set options from UpdateOptions
                UpdateOptions opts = update.getOptions();
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.collation(ffiModel,
                    toCollationBson(arena, opts.getCollation()));
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.hint(ffiModel,
                    toHintBsonValue(arena, opts.getHint(), opts.getHintString()));
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.upsert(ffiModel,
                    toBooleanByte(opts.isUpsert()));
            } else if (model instanceof ReplaceOneModel) {
                ReplaceOneModel<BsonDocument> replace = (ReplaceOneModel<BsonDocument>) model;
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.operation(ffiModel, OP_REPLACE_ONE);
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.document(ffiModel,
                    BsonMarshaller.toBsonStruct(arena, replace.getFilter()));
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.update(ffiModel,
                    BsonMarshaller.toBsonStruct(arena, replace.getReplacement()));
                // Set options from ReplaceOptions
                ReplaceOptions opts = replace.getReplaceOptions();
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.collation(ffiModel,
                    toCollationBson(arena, opts.getCollation()));
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.hint(ffiModel,
                    toHintBsonValue(arena, opts.getHint(), opts.getHintString()));
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.upsert(ffiModel,
                    toBooleanByte(opts.isUpsert()));
            } else if (model instanceof DeleteOneModel) {
                DeleteOneModel<BsonDocument> delete = (DeleteOneModel<BsonDocument>) model;
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.operation(ffiModel, OP_DELETE_ONE);
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.document(ffiModel,
                    BsonMarshaller.toBsonStruct(arena, delete.getFilter()));
                // Set options from DeleteOptions
                DeleteOptions opts = delete.getOptions();
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.collation(ffiModel,
                    toCollationBson(arena, opts.getCollation()));
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.hint(ffiModel,
                    toHintBsonValue(arena, opts.getHint(), opts.getHintString()));
            } else if (model instanceof DeleteManyModel) {
                DeleteManyModel<BsonDocument> delete = (DeleteManyModel<BsonDocument>) model;
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.operation(ffiModel, OP_DELETE_MANY);
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.document(ffiModel,
                    BsonMarshaller.toBsonStruct(arena, delete.getFilter()));
                // Set options from DeleteOptions
                DeleteOptions opts = delete.getOptions();
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.collation(ffiModel,
                    toCollationBson(arena, opts.getCollation()));
                com.mongodb.internal.rust.crud.ffi.BulkWriteModel.hint(ffiModel,
                    toHintBsonValue(arena, opts.getHint(), opts.getHintString()));
            }
        }

        return new WriteModelArray(array, models.size());
    }

    /**
     * Helper class to hold a WriteModel array pointer and its length.
     */
    public static class WriteModelArray {
        public final MemorySegment array;
        public final long length;

        public WriteModelArray(MemorySegment array, long length) {
            this.array = array;
            this.length = length;
        }
    }

    /**
     * Creates ListDatabasesOptions FFI struct.
     */
    public static MemorySegment toListDatabasesOptions(Arena arena, ListDatabasesOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.ListDatabasesOptions.allocate(arena);

        com.mongodb.internal.rust.crud.ffi.ListDatabasesOptions.filter(opts,
            toBsonOrNull(arena, options.getFilter()));
        com.mongodb.internal.rust.crud.ffi.ListDatabasesOptions.name_only(opts,
            toBooleanByte(options.getNameOnly()));
        com.mongodb.internal.rust.crud.ffi.ListDatabasesOptions.authorized_databases(opts,
            toBooleanByte(options.getAuthorizedDatabasesOnly()));
        com.mongodb.internal.rust.crud.ffi.ListDatabasesOptions.comment(opts,
            toCommentBsonValue(arena, options.getComment()));
        // Note: batchSize and maxTimeMS not currently in FFI

        return opts;
    }

    /**
     * Creates ListCollectionsOptions FFI struct.
     */
    public static MemorySegment toListCollectionsOptions(Arena arena, ListCollectionsOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.ListCollectionsOptions.allocate(arena);

        com.mongodb.internal.rust.crud.ffi.ListCollectionsOptions.batch_size(opts, options.getBatchSize());
        // Note: filter, comment, maxTimeMS, authorizedCollections not currently in FFI

        return opts;
    }

    /**
     * Creates ListIndexesOptions FFI struct.
     */
    public static MemorySegment toListIndexesOptions(Arena arena, ListIndexesOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.ListIndexesOptions.allocate(arena);

        com.mongodb.internal.rust.crud.ffi.ListIndexesOptions.batch_size(opts, options.getBatchSize());
        // Note: comment, maxTimeMS not currently in FFI

        return opts;
    }
}

