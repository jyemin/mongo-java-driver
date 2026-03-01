# FFI Architecture Notes

This document captures architectural mismatches discovered while implementing the Java FFI bindings for the Rust MongoDB driver.

## 1. Return Types Are Not Cursors

**Assumed:** Operations like `listDatabases`, `listCollectionNames`, `listDatabaseNames`, and `distinct` would return cursors (like the existing Java driver).

**Reality:** The FFI returns single result structs containing arrays:
- `ListDatabasesResult` → `{ databases: Bson, total_size: uint64 }`
- `ListCollectionNamesResult` → `{ names: Bson }` (array in a doc)
- `ListDatabaseNamesResult` → `{ names: Bson }` (array in a doc)
- `DistinctResult` → `{ values: Bson }` (array in a doc)

**Solution:** Created `ListBackedCursor<T>` to wrap the pre-fetched list and present it as an `AsyncCursor`.

---

## 2. Array Parameters vs Batch Structs

**Assumed:** Operations taking multiple items (like `createIndexes`, `bulkWrite`) would take a `BsonBatch` struct.

**Reality:** They take a raw array pointer + length:
```c
void mongo_create_indexes(client, ctx, db, coll, IndexModel* indexes, size_t indexes_len, ...)
void mongo_bulk_write(client, ctx, db, coll, BulkWriteModel* models, size_t models_len, ...)
```

**Solution:** Created `IndexModelArray` and `WriteModelArray` helper classes in `OptionsMarshaller` to hold both the pointer and length.

---

## 3. Rename Collection Has No Options Struct

**Assumed:** `renameCollection` would take a `RenameCollectionOptions` struct (like other operations).

**Reality:** It takes `drop_target` as a direct `bool` parameter, and only renames within the same database:
```c
void mongo_rename_collection(client, ctx, db_name, coll_name, new_name, bool drop_target, ...)
```

**Solution:** Extracted `drop_target` from the Java `RenameCollectionOptions` and passed it directly. Added validation that source and target databases match.

---

## 4. Filter Location Varies

**Assumed:** Filter would be a direct parameter to `listDatabases` / `listDatabaseNames`.

**Reality:** Filter is inside `ListDatabasesOptions` struct, not a separate parameter:
```c
void mongo_list_databases(client, ctx, ListDatabasesOptions* options, ...)
```

**Solution:** Created `toListDatabasesOptions(arena, filter)` that populates the options struct with the filter.

---

## 5. Callback Types Are Operation-Specific

**Assumed:** There would be generic callbacks like `StringCallback`, `DocumentCallback`.

**Reality:** Each operation has its own specific callback type:
- `CreateIndexCallback` → `CreateIndexResult` (with `index_name`)
- `CreateIndexesCallback` → `CreateIndexesResult` (with `index_names` as Bson array)
- `CommandCallback` → `CommandResult` (with `response`)
- `ListDatabasesCallback` → `ListDatabasesResult` (with `databases` + `total_size`)

**Solution:** Used the operation-specific callbacks and extracted the relevant fields from their result structs in `CallbackBridge`.

---

## 6. Missing `userdata` Parameter

**Assumed:** FFI calls would not require a userdata parameter.

**Reality:** Almost every FFI function has a `userdata` parameter for context passing to callbacks:
```c
void mongo_list_indexes(client, ctx, db, coll, options, void* userdata, callback)
```

**Solution:** Added `MemorySegment.NULL` for userdata in all calls.

---

## 7. FFI Option Struct Fields Don't Match Java

**Assumed:** FFI option structs would have fields like `max_time_ms`, `let$`.

**Reality:**
- Many structs don't have `max_time_ms` (handled differently in Rust)
- Field names use `let_vars` not `let$` (jextract doesn't escape `$` in generated names)
- Some expected fields simply don't exist

**Solution:** Verified each FFI struct by examining the generated code:
```bash
grep "Setter for field" rust-crud-bindings/src/ffm-generated/.../SomeOptions.java
```

---

## Verification Process

Before implementing any operation:

1. **Check the FFI function signature:**
   ```bash
   grep "public static void mongo_operation_name" MongoDbFfi_1.java
   ```

2. **Check the callback type and result struct:**
   ```bash
   ls *Callback*.java | grep OperationName
   cat OperationNameResult.java
   ```

3. **Check available options struct fields:**
   ```bash
   grep "Setter for field" OperationNameOptions.java
   ```

---

## Key Lesson

**Don't assume - verify.** The FFI is generated from a C header, and the Rust driver's C API doesn't mirror the Java driver's API. Each operation needs its signature verified against the actual generated code in `MongoDbFfi_1.java`.

---

## Numbered Gap List

All missing features, numbered for reference.

### OperationContext Gaps
- ~~**GAP-1**: ClientSession mapping~~ ✅ DONE (RustClientSessionBase.getSessionPtr())
- ~~**GAP-2**: ReadConcern not mapped~~ ✅ DONE
- ~~**GAP-3**: ReadPreference not mapped~~ ✅ DONE
- ~~**GAP-4**: WriteConcern not mapped~~ ✅ DONE
- ~~**GAP-5**: timeout_ms not mapped~~ ✅ DONE (createOperationContext now accepts timeoutMs)

### Session/Transaction Support
- ~~**GAP-6**: SessionOptions - causal_consistency, default_transaction_options, snapshot~~ ✅ DONE (toSessionOptions)
- ~~**GAP-7**: TransactionOptions - max_commit_time_ms, read/write concern settings~~ ✅ DONE (toTransactionOptions)
- ~~**GAP-8**: ReadConcernOptions - level~~ ✅ DONE (toReadConcernOptions)
- ~~**GAP-9**: WriteConcernOptions - journal, w, w_tag, w_timeout_ms~~ ✅ DONE (toWriteConcernOptions)
- ~~**GAP-10**: ReadPreferenceOptions - hedge, max_staleness_seconds, mode, tags~~ ✅ DONE (toReadPreferenceOptions)

### ReplaceOptions
- ~~**GAP-11**: bypass_document_validation~~ ✅ DONE
- ~~**GAP-12**: hint~~ ✅ DONE
- ~~**GAP-13**: upsert~~ ✅ DONE
- ~~**GAP-14**: comment~~ ✅ DONE

### DeleteOptions
- ~~**GAP-15**: hint~~ ✅ DONE
- ~~**GAP-16**: comment~~ ✅ DONE

### FindOneOptions
- ~~**GAP-17**: hint~~ ✅ DONE
- ~~**GAP-18**: comment~~ ✅ DONE

### FindOptions
- ~~**GAP-19**: hint~~ ✅ DONE
- ~~**GAP-20**: comment~~ ✅ DONE

### FindOneAndUpdateOptions
- ~~**GAP-21**: projection~~ ✅ DONE
- ~~**GAP-22**: sort~~ ✅ DONE
- ~~**GAP-23**: upsert~~ ✅ DONE
- ~~**GAP-24**: return_document~~ ✅ DONE
- ~~**GAP-25**: bypass_document_validation~~ ✅ DONE
- **GAP-26**: array_filters - ⚠️ BLOCKED: FFI uses `Bson*` but should be `BsonBatch` like pipeline
- ~~**GAP-27**: hint~~ ✅ DONE
- ~~**GAP-28**: comment~~ ✅ DONE

### FindOneAndReplaceOptions
- ~~**GAP-29**: projection~~ ✅ DONE
- ~~**GAP-30**: sort~~ ✅ DONE
- ~~**GAP-31**: upsert~~ ✅ DONE
- ~~**GAP-32**: return_document~~ ✅ DONE
- ~~**GAP-33**: bypass_document_validation~~ ✅ DONE
- ~~**GAP-34**: hint~~ ✅ DONE
- ~~**GAP-35**: comment~~ ✅ DONE

### FindOneAndDeleteOptions
- ~~**GAP-36**: projection~~ ✅ DONE
- ~~**GAP-37**: sort~~ ✅ DONE
- ~~**GAP-38**: hint~~ ✅ DONE
- ~~**GAP-39**: comment~~ ✅ DONE

### AggregateOptions
- ~~**GAP-40**: bypass_document_validation~~ ✅ DONE (added as separate API parameter)
- ~~**GAP-41**: hint~~ ✅ DONE
- ~~**GAP-42**: comment~~ ✅ DONE
- ~~**GAP-43**: let_vars~~ ✅ DONE

### CountOptions
- ~~**GAP-44**: hint~~ ✅ DONE
- ~~**GAP-45**: comment~~ ✅ DONE

### DistinctOptions
- ~~**GAP-46**: comment~~ ✅ DONE

### BulkWriteOptions
- ~~**GAP-47**: bypass_document_validation~~ ✅ DONE (was already done)
- ~~**GAP-48**: comment~~ ✅ DONE

### ChangeStreamOptions
- ~~**GAP-49**: full_document~~ ✅ DONE
- ~~**GAP-50**: full_document_before_change~~ ✅ DONE
- ~~**GAP-51**: resume_after~~ ✅ DONE
- ~~**GAP-52**: start_after~~ ✅ DONE
- ~~**GAP-53**: start_at_operation_time~~ ✅ DONE
- ~~**GAP-54**: comment~~ ✅ DONE

### ListDatabasesOptions
- ~~**GAP-55**: authorized_databases~~ ✅ DONE (full API support)
- ~~**GAP-56**: name_only~~ ✅ DONE (full API support)
- ~~**GAP-57**: comment~~ ✅ DONE (full API support)

### InsertOneOptions
- ~~**GAP-58**: bypass_document_validation~~ ✅ DONE
- ~~**GAP-59**: comment~~ ✅ DONE

### InsertManyOptions
- ~~**GAP-60**: bypass_document_validation~~ ✅ DONE
- ~~**GAP-61**: comment~~ ✅ DONE
- ~~**GAP-62**: ordered~~ ✅ DONE

### CreateCollectionOptions
- ~~**GAP-63**: capped~~ ✅ DONE
- ~~**GAP-64**: collation~~ ✅ DONE
- ~~**GAP-65**: expire_after_seconds~~ ✅ DONE
- ~~**GAP-66**: max~~ ✅ DONE
- ~~**GAP-67**: size~~ ✅ DONE
- ~~**GAP-68**: storage_engine~~ ✅ DONE
- ~~**GAP-69**: timeseries~~ ✅ DONE
- ~~**GAP-70**: validation_action~~ ✅ DONE
- ~~**GAP-71**: validation_level~~ ✅ DONE
- ~~**GAP-72**: validator~~ ✅ DONE

### IndexModel / IndexOptions
- ~~**GAP-73**: IndexModel.options not mapped~~ ✅ DONE
- ~~**GAP-74**: IndexOptions.background~~ ✅ DONE
- ~~**GAP-75**: IndexOptions.collation~~ ✅ DONE
- ~~**GAP-76**: IndexOptions.expire_after_seconds (TTL)~~ ✅ DONE
- ~~**GAP-77**: IndexOptions.hidden~~ ✅ DONE
- ~~**GAP-78**: IndexOptions.name~~ ✅ DONE
- ~~**GAP-79**: IndexOptions.partial_filter_expression~~ ✅ DONE
- ~~**GAP-80**: IndexOptions.sparse~~ ✅ DONE
- ~~**GAP-81**: IndexOptions.unique~~ ✅ DONE
- ~~**GAP-82**: IndexOptions.wildcard_projection~~ ✅ DONE

### BulkWriteModel Per-Model Options
- ~~**GAP-83**: Per-model collation~~ ✅ DONE
- ~~**GAP-84**: Per-model hint~~ ✅ DONE
- **GAP-85**: Per-model array_filters - ⚠️ BLOCKED: FFI uses `Bson*` but should be `BsonBatch`

---

## File Summary

### Public API
- `AsyncOperationExecutor` - Interface defining all async operations
- `SyncOperationExecutor` - Blocking wrapper for sync usage
- `AsyncCursor<T>` / `SyncCursor<T>` - Cursor interfaces
- `AsyncChangeStream<T>` / `SyncChangeStream<T>` - Change stream interfaces
- `SingleResultCallback<T>` - Callback interface

### Implementation
- `RustAsyncOperationExecutor` - FFI-backed async implementation
- `RustAsyncCursor` - FFI-backed cursor implementation
- `RustAsyncChangeStream` - FFI-backed change stream implementation
- `ListBackedCursor<T>` - Simple cursor wrapping a pre-fetched list

### Internal Utilities
- `BsonMarshaller` - BSON serialization/deserialization to/from FFI
- `CallbackBridge` - Creates FFI callback stubs
- `CursorHandle` - Wraps FFI cursor results
- `ErrorConverter` - Converts FFI errors to Java exceptions
- `OptionsMarshaller` - Converts Java options to FFI structs
- `ResultConverter` - Converts FFI results to Java objects

