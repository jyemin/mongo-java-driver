# Operation Executor Redesign

## Problem

The current architecture has multiple layers for executing operations:

1. **`Operations`** - Factory class that creates operation objects
2. **`*Operation` classes** - ~90 classes that know how to build commands and execute via binding
3. **`OperationExecutor`** - Executes operations, handles session management

```java
// Current pattern in MongoCollectionImpl
return getExecutor(timeoutSettings)
    .execute(operations.insertOne(document, options), readConcern, clientSession);
```

With FFI handling command building, retry logic, batching, and spec compliance in Rust, this complexity becomes unnecessary.

## New Design

Replace the Operation abstraction with direct operation methods on `OperationExecutor`.

### Core Interface (driver-core)

Callback-based for sharing between sync and reactive drivers:

```java
public interface AsyncOperationExecutor {
    
    void insertOne(MongoNamespace namespace,
                   BsonDocument document,
                   InsertOneOptions options,
                   @Nullable ClientSession session,
                   SingleResultCallback<InsertOneResult> callback);
    
    void bulkWrite(MongoNamespace namespace,
                   List<WriteModel<?>> models,
                   BulkWriteOptions options,
                   @Nullable ClientSession session,
                   SingleResultCallback<BulkWriteResult> callback);
    
    // ... other operations
}

@FunctionalInterface
public interface SingleResultCallback<T> {
    void onResult(@Nullable T result, @Nullable Throwable error);
}
```

### Sync Interface (driver-sync)

```java
public interface OperationExecutor {
    
    InsertOneResult insertOne(MongoNamespace namespace,
                              BsonDocument document,
                              InsertOneOptions options,
                              @Nullable ClientSession session);
    
    BulkWriteResult bulkWrite(MongoNamespace namespace,
                              List<WriteModel<?>> models,
                              BulkWriteOptions options,
                              @Nullable ClientSession session);
    
    // ... other operations
}
```

### Reactive Interface (driver-reactive-streams)

```java
public interface ReactiveOperationExecutor {
    
    Mono<InsertOneResult> insertOne(MongoNamespace namespace,
                                    BsonDocument document,
                                    InsertOneOptions options,
                                    @Nullable ClientSession session);
    
    Mono<BulkWriteResult> bulkWrite(MongoNamespace namespace,
                                    List<WriteModel<?>> models,
                                    BulkWriteOptions options,
                                    @Nullable ClientSession session);
    
    // ... other operations
}
```

## Implementations

### RustAsyncOperationExecutor (driver-core or rust-bindings)

Implements `AsyncOperationExecutor` using FFI calls to Rust. Single implementation shared by both drivers.

### SyncOperationExecutor (driver-sync)

Wraps `AsyncOperationExecutor`, blocks on callbacks:

```java
public class SyncOperationExecutor implements OperationExecutor {
    private final AsyncOperationExecutor async;
    
    public InsertOneResult insertOne(MongoNamespace ns, BsonDocument doc, 
                                     InsertOneOptions opts, ClientSession session) {
        CompletableFuture<InsertOneResult> future = new CompletableFuture<>();
        async.insertOne(ns, doc, opts, session, (result, error) -> {
            if (error != null) future.completeExceptionally(error);
            else future.complete(result);
        });
        return future.join();
    }
}
```

### ReactorOperationExecutor (driver-reactive-streams)

Wraps `AsyncOperationExecutor`, returns `Mono` with lazy subscription:

```java
public class ReactorOperationExecutor implements ReactiveOperationExecutor {
    private final AsyncOperationExecutor async;
    
    public Mono<InsertOneResult> insertOne(MongoNamespace ns, BsonDocument doc,
                                           InsertOneOptions opts, ClientSession session) {
        return Mono.create(sink -> {
            async.insertOne(ns, doc, opts, session, (result, error) -> {
                if (error != null) sink.error(error);
                else sink.success(result);
            });
        });
    }
}
```

## Benefits

1. **Simplified MongoCollectionImpl** - Direct calls instead of operation factory pattern:
   ```java
   // Before
   return getExecutor(timeoutSettings)
       .execute(operations.insertOne(document, options), readConcern, clientSession);
   
   // After  
   return executor.insertOne(namespace, toBson(document), options, clientSession);
   ```

2. **~90 Operation classes become obsolete** - For FFI mode, all operation logic is in Rust

3. **Operations class removed from MongoCollectionImpl** - No more factory

4. **Shared implementation** - Single FFI implementation in driver-core, wrapped by sync/reactive

5. **No Project Reactor dependency in sync driver** - Separate interfaces

## Migration

- `JavaOperationExecutor` can wrap existing `Operations` + old execute pattern for non-FFI mode
- Gradual migration: implement operations one at a time in `RustAsyncOperationExecutor`

