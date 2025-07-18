/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.UnixServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.model.Filters;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.client.unified.UnifiedTestModifications.TestDef;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.TestServerMonitorListener;
import com.mongodb.internal.connection.TestClusterListener;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import com.mongodb.internal.logging.LogMessage;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import com.mongodb.logging.TestLoggingInterceptor;
import com.mongodb.test.AfterBeforeParameterResolver;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.assertions.Assertions;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.diagnostics.Logger;
import org.bson.diagnostics.Loggers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentest4j.TestAbortedException;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.getServerVersion;
import static com.mongodb.ClusterFixture.isDataLakeTest;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.test.CollectionHelper.getCurrentClusterTime;
import static com.mongodb.client.test.CollectionHelper.killAllSessions;
import static com.mongodb.client.unified.RunOnRequirementsMatcher.runOnRequirementsMet;
import static com.mongodb.client.unified.UnifiedTestModifications.Modifier;
import static com.mongodb.client.unified.UnifiedTestModifications.applyCustomizations;
import static com.mongodb.client.unified.UnifiedTestModifications.testDef;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static util.JsonPoweredTestHelper.getSpecTestDocuments;

@ExtendWith(AfterBeforeParameterResolver.class)
public abstract class UnifiedTest {
    private static final Logger LOGGER = Loggers.getLogger("UnifiedTest");

    private static final Set<String> PRESTART_POOL_ASYNC_WORK_MANAGER_FILE_DESCRIPTIONS = Collections.singleton(
            "wait queue timeout errors include details about checked out connections");

    private static final String MAX_SUPPORTED_SCHEMA_VERSION = "1.22";
    private static final List<Integer> MAX_SUPPORTED_SCHEMA_VERSION_COMPONENTS = Arrays.stream(MAX_SUPPORTED_SCHEMA_VERSION.split("\\."))
            .map(Integer::parseInt)
            .collect(Collectors.toList());

    private static final String TOPOLOGY_CLOSED_EVENT = "topologyClosedEvent";
    private static final List<String> TOPOLOGY_EVENT_NAMES = asList("topologyOpeningEvent", "topologyDescriptionChangedEvent",
            TOPOLOGY_CLOSED_EVENT);

    public static final int RETRY_ATTEMPTS = 3;
    public static final int FORCE_FLAKY_ATTEMPTS = 10;
    private static final Set<String> ATTEMPTED_TESTS_TO_HENCEFORTH_IGNORE = new HashSet<>();

    @Nullable
    private String fileDescription;
    private String schemaVersion;
    @Nullable
    private BsonArray runOnRequirements;
    private BsonArray entitiesArray;
    private BsonArray initialData;
    private BsonDocument definition;
    private Entities entities;
    private UnifiedCrudHelper crudHelper;
    private UnifiedGridFSHelper gridFSHelper;
    private UnifiedClientEncryptionHelper clientEncryptionHelper;
    private List<FailPoint> failPoints;
    private UnifiedTestContext rootContext;
    private boolean ignoreExtraEvents;
    private BsonDocument startingClusterTime;
    @Nullable
    private TestDef testDef;

    private class UnifiedTestContext {
        private final AssertionContext context = new AssertionContext();
        private final ValueMatcher valueMatcher = new ValueMatcher(entities, context);
        private final ErrorMatcher errorMatcher = new ErrorMatcher(context, valueMatcher);
        private final EventMatcher eventMatcher = new EventMatcher(valueMatcher, context);
        private final LogMatcher logMatcher = new LogMatcher(valueMatcher, context);

        AssertionContext getAssertionContext() {
            return context;
        }

        ValueMatcher getValueMatcher() {
            return valueMatcher;
        }

        ErrorMatcher getErrorMatcher() {
            return errorMatcher;
        }

        EventMatcher getEventMatcher() {
            return eventMatcher;
        }

        LogMatcher getLogMatcher() {
            return logMatcher;
        }
    }

    protected UnifiedTest() {
    }

    public Entities getEntities() {
        return entities;
    }

    @NonNull
    protected static Collection<Arguments> getTestData(final String directory, final boolean isReactive, final Language language) {
        List<Arguments> data = new ArrayList<>();

        for (BsonDocument fileDocument : getSpecTestDocuments(directory)) {
            if (!fileDocument.containsKey("schemaVersion")) {
                LOGGER.info("Not a unified test file: " + fileDocument.getString("fileName").getValue());
                continue;
            }
            String fileDescription = fileDocument.getString("description").getValue();
            for (BsonValue cur : fileDocument.getArray("tests")) {
                final BsonDocument testDocument = cur.asDocument();
                String testDescription = testDocument.getString("description").getValue();
                TestDef testDef = testDef(directory, fileDescription, testDescription, isReactive, language);
                applyCustomizations(testDef);

                boolean forceFlaky = testDef.wasAssignedModifier(Modifier.FORCE_FLAKY);
                boolean retry = forceFlaky || testDef.wasAssignedModifier(Modifier.RETRY);

                int attempts;
                if (retry) {
                    attempts = forceFlaky ? FORCE_FLAKY_ATTEMPTS : RETRY_ATTEMPTS;
                } else {
                    attempts = 1;
                }

                for (int attempt = 1; attempt <= attempts; attempt++) {
                    String testName = MessageFormat.format("{0}: {1}", fileDescription, testDescription);
                    data.add(Arguments.of(
                            testName,
                            fileDescription,
                            testDescription,
                            directory,
                            attempt,
                            attempts,
                            fileDocument.getString("schemaVersion").getValue(),
                            fileDocument.getArray("runOnRequirements", null),
                            fileDocument.getArray("createEntities", new BsonArray()),
                            fileDocument.getArray("initialData", new BsonArray()),
                            testDocument.clone()));
                }
            }
        }
        return data;
    }

    protected BsonDocument getDefinition() {
        return definition;
    }

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    protected abstract GridFSBucket createGridFSBucket(MongoDatabase database);

    protected abstract ClientEncryption createClientEncryption(MongoClient keyVaultClient, ClientEncryptionSettings clientEncryptionSettings);

    @BeforeEach
    public void setUp(
            final String testName,
            @Nullable final String fileDescription,
            @Nullable final String testDescription,
            @Nullable final String directoryName,
            final int attemptNumber,
            final int totalAttempts,
            final String schemaVersion,
            @Nullable final BsonArray runOnRequirements,
            final BsonArray entitiesArray,
            final BsonArray initialData,
            final BsonDocument definition) {
        this.fileDescription = fileDescription;
        this.schemaVersion = schemaVersion;
        this.runOnRequirements = runOnRequirements;
        this.entitiesArray = entitiesArray;
        this.initialData = initialData;
        this.definition = definition;
        entities = new Entities();
        crudHelper = new UnifiedCrudHelper(entities, definition.getString("description").getValue());
        gridFSHelper = new UnifiedGridFSHelper(entities);
        clientEncryptionHelper = new UnifiedClientEncryptionHelper(entities);
        failPoints = new ArrayList<>();
        rootContext = new UnifiedTestContext();
        rootContext.getAssertionContext().push(ContextElement.ofTest(definition));
        ignoreExtraEvents = false;
        if (directoryName != null && fileDescription != null && testDescription != null) {
            testDef = testDef(directoryName, fileDescription, testDescription, isReactive(), getLanguage());
            applyCustomizations(testDef);

            boolean skip = testDef.wasAssignedModifier(Modifier.SKIP);
            assumeFalse(skip, "Skipping test");
        }
        skips(fileDescription, testDescription);

        assumeTrue(isSupportedSchemaVersion(schemaVersion), format("Unsupported schema version %s", schemaVersion));

        if (runOnRequirements != null) {
            assumeTrue(runOnRequirementsMet(runOnRequirements, getMongoClientSettings(), getServerVersion()),
                    "Run-on requirements not met");
        }
        if (definition.containsKey("runOnRequirements")) {
            assumeTrue(runOnRequirementsMet(definition.getArray("runOnRequirements", new BsonArray()), getMongoClientSettings(),
                            getServerVersion()),
                    "Run-on requirements not met");
        }
        if (definition.containsKey("skipReason")) {
            throw new TestAbortedException(definition.getString("skipReason").getValue());
        }

        if (!isDataLakeTest()) {
            killAllSessions();
        }

        startingClusterTime = addInitialDataAndGetClusterTime();

        entities.init(entitiesArray, startingClusterTime,
                fileDescription != null && PRESTART_POOL_ASYNC_WORK_MANAGER_FILE_DESCRIPTIONS.contains(fileDescription),
                this::createMongoClient,
                this::createGridFSBucket,
                this::createClientEncryption);
        if (testDef != null) {
            postSetUp(testDef);
        }
    }

    protected void postSetUp(final TestDef def) {
    }

    @AfterEach
    public void cleanUp() {
        for (FailPoint failPoint : failPoints) {
            failPoint.disableFailPoint();
        }
        entities.close();
        if (testDef != null) {
            postCleanUp(testDef);
        }
    }

    protected void postCleanUp(final TestDef testDef) {
    }

    /**
     * This method is called once per
     * {@link #setUp(String, String, String, String, int, int, String, org.bson.BsonArray, org.bson.BsonArray, org.bson.BsonArray, org.bson.BsonDocument)}, unless
     * {@link #setUp(String, String, String, String, int, int, String, org.bson.BsonArray, org.bson.BsonArray, org.bson.BsonArray, org.bson.BsonDocument)} fails unexpectedly.
     */
    protected void skips(final String fileDescription, final String testDescription) {
    }

    protected boolean isReactive() {
        return false;
    }

    protected Language getLanguage() {
        return Language.JAVA;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("data")
    public void shouldPassAllOutcomes(
            final String testName,
            @Nullable final String fileDescription,
            @Nullable final String testDescription,
            @Nullable final String directoryName,
            final int attemptNumber,
            final int totalAttempts,
            final String schemaVersion,
            @Nullable final BsonArray runOnRequirements,
            final BsonArray entitiesArray,
            final BsonArray initialData,
            final BsonDocument definition) {
        boolean forceFlaky = testDef.wasAssignedModifier(Modifier.FORCE_FLAKY);
        if (!forceFlaky) {
            boolean ignoreThisTest = ATTEMPTED_TESTS_TO_HENCEFORTH_IGNORE.contains(testName);
            assumeFalse(ignoreThisTest, "Skipping a retryable test that already succeeded");
            // The attempt is what counts, since a test may fail with
            // something like "ignored", and would not be retried.
            // Only failures should trigger another attempt.
            ATTEMPTED_TESTS_TO_HENCEFORTH_IGNORE.add(testName);
        }
        try {
            BsonArray operations = definition.getArray("operations");
            for (int i = 0; i < operations.size(); i++) {
                BsonValue cur = operations.get(i);
                assertOperation(rootContext, cur.asDocument(), i);
            }

            if (definition.containsKey("outcome")) {
                assertOutcome(rootContext);
            }

            if (definition.containsKey("expectEvents")) {
                compareEvents(rootContext, definition);
            }

            if (definition.containsKey("expectLogMessages")) {
                ArrayList<LogMatcher.Tweak> tweaks = new ArrayList<>(singletonList(
                        // `LogMessage.Entry.Name.OPERATION` is not supported, therefore we skip matching its value
                        LogMatcher.Tweak.skip(LogMessage.Entry.Name.OPERATION)));
                if (getMongoClientSettings().getClusterSettings()
                        .getHosts().stream().anyMatch(serverAddress -> serverAddress instanceof UnixServerAddress)) {
                    tweaks.add(LogMatcher.Tweak.skip(LogMessage.Entry.Name.SERVER_PORT));
                }
                compareLogMessages(rootContext, definition, tweaks);
            }
        } catch (TestAbortedException e) {
            // if a test is ignored, we do not retry
            throw e;
        } catch (Throwable e) {
            if (forceFlaky) {
                throw e;
            }
            if (testDef != null && !testDef.matchesThrowable(e)) {
                // if the throwable is not matched, test definitions were not intended to apply; rethrow it
                throw e;
            }
            boolean isLastAttempt = attemptNumber == totalAttempts;
            if (isLastAttempt) {
                throw e;
            }

            ATTEMPTED_TESTS_TO_HENCEFORTH_IGNORE.remove(testName);
            abort("Ignoring failure and retrying attempt " + attemptNumber);
        }
    }

    private void compareEvents(final UnifiedTestContext context, final BsonDocument definition) {
        for (BsonValue cur : definition.getArray("expectEvents")) {
            BsonDocument curClientEvents = cur.asDocument();
            String client = curClientEvents.getString("client").getValue();
            boolean ignoreExtraEvents =
                    curClientEvents.getBoolean("ignoreExtraEvents", BsonBoolean.valueOf(this.ignoreExtraEvents)).getValue();
            String eventType = curClientEvents.getString("eventType", new BsonString("command")).getValue();
            BsonArray expectedEvents = curClientEvents.getArray("events");
            if (eventType.equals("command")) {
                TestCommandListener listener = entities.getClientCommandListener(client);
                context.getEventMatcher().assertCommandEventsEquality(client, ignoreExtraEvents, expectedEvents,
                        listener.getEvents());
            } else if (eventType.equals("cmap")) {
                TestConnectionPoolListener listener = entities.getConnectionPoolListener(client);
                context.getEventMatcher().assertConnectionPoolEventsEquality(client, ignoreExtraEvents, expectedEvents,
                        listener.getEvents());
            } else if (eventType.equals("sdam")) {

                // SDAM tests also include topology events, so we need to separate them to be able to assert them separately.
                // Partition the expected events into two lists with the key being if it's a topology based event or not.
                Map<Boolean, List<BsonDocument>> partitionedEventsMap = expectedEvents.stream()
                        .map(BsonValue::asDocument)
                        .collect(Collectors.partitioningBy(doc -> TOPOLOGY_EVENT_NAMES.stream().anyMatch(doc::containsKey)));

                BsonArray expectedTopologyEvents = new BsonArray(partitionedEventsMap.get(true));
                if (!expectedTopologyEvents.isEmpty()) {
                    TestClusterListener clusterListener = entities.getClusterListener(client);
                    // Unfortunately, some tests expect the cluster to be closed, but do not define it as a waitForEvent in the spec -
                    // causing a race condition in the test.
                    if (expectedTopologyEvents.stream().anyMatch(doc -> doc.asDocument().containsKey(TOPOLOGY_CLOSED_EVENT))) {
                        context.getEventMatcher().waitForClusterClosedEvent(client, clusterListener);
                    }

                    List<Object> topologyEvents = new ArrayList<>();
                    topologyEvents.add(clusterListener.getClusterOpeningEvent());
                    topologyEvents.addAll(clusterListener.getClusterDescriptionChangedEvents());
                    topologyEvents.add(clusterListener.getClusterClosingEvent());
                    context.getEventMatcher().assertTopologyEventsEquality(client, ignoreExtraEvents, expectedTopologyEvents, topologyEvents);
                }

                BsonArray expectedSdamEvents = new BsonArray(partitionedEventsMap.get(false));
                if (!expectedSdamEvents.isEmpty()) {
                    TestServerMonitorListener serverMonitorListener = entities.getServerMonitorListener(client);
                    context.getEventMatcher().assertServerMonitorEventsEquality(client, ignoreExtraEvents, expectedSdamEvents, serverMonitorListener.getEvents());
                }
            } else {
                throw new UnsupportedOperationException("Unexpected event type: " + eventType);
            }
        }
    }

    private boolean isSupportedSchemaVersion(final String schemaVersion) {
        List<Integer> schemaVersionComponents = Arrays.stream(schemaVersion.split("\\."))
                .map(Integer::parseInt)
                .collect(Collectors.toList());

        if (schemaVersionComponents.size() != 2) {
            Assertions.fail("Unsupported schema version: " + schemaVersion);
        } else if (schemaVersionComponents.get(0) < 1) {
            Assertions.fail("Unsupported schema version: " + schemaVersion);
        }

        for (int i = 0; i < 2; i++){
            int schemaComponent = schemaVersionComponents.get(i);
            int maxSupportedComponent = MAX_SUPPORTED_SCHEMA_VERSION_COMPONENTS.get(i);
            if (schemaComponent > maxSupportedComponent) {
                return false;
            }
        }
        return true;
    }

    private void compareLogMessages(final UnifiedTestContext rootContext, final BsonDocument definition,
            final Iterable<LogMatcher.Tweak> tweaks) {
        for (BsonValue cur : definition.getArray("expectLogMessages")) {
            BsonDocument curLogMessagesForClient = cur.asDocument();
            boolean ignoreExtraMessages = curLogMessagesForClient.getBoolean("ignoreExtraMessages", BsonBoolean.FALSE).getValue();
            String clientId = curLogMessagesForClient.getString("client").getValue();
            TestLoggingInterceptor loggingInterceptor =
                    entities.getClientLoggingInterceptor(clientId);
            rootContext.getLogMatcher().assertLogMessageEquality(clientId, ignoreExtraMessages,
                    curLogMessagesForClient.getArray("messages"), loggingInterceptor.getMessages(), tweaks);
        }
    }

    private void assertOutcome(final UnifiedTestContext context) {
        for (BsonValue cur : definition.getArray("outcome")) {
            BsonDocument curDocument = cur.asDocument();
            MongoNamespace namespace = new MongoNamespace(curDocument.getString("databaseName").getValue(),
                    curDocument.getString("collectionName").getValue());
            List<BsonDocument> expectedOutcome = curDocument.getArray("documents").stream().map(BsonValue::asDocument).collect(toList());
            List<BsonDocument> actualOutcome = new CollectionHelper<>(new BsonDocumentCodec(), namespace).find();
            context.getAssertionContext().push(ContextElement.ofOutcome(namespace, expectedOutcome, actualOutcome));
            assertEquals(expectedOutcome, actualOutcome, context.getAssertionContext().getMessage("Outcomes are not equal"));
            context.getAssertionContext().pop();
        }
    }

    private void assertOperationAndThrow(final UnifiedTestContext context, final BsonDocument operation, final int operationIndex) {
        OperationResult result = executeOperation(context, operation, operationIndex);
        assertOperationResult(context, operation, operationIndex, result);

        if (result.getException() != null) {
            throw (RuntimeException) result.getException();
        }
    }

    private void assertOperation(final UnifiedTestContext context, final BsonDocument operation, final int operationIndex) {
        OperationResult result = executeOperation(context, operation, operationIndex);
        assertOperationResult(context, operation, operationIndex, result);
    }

    private static void assertOperationResult(final UnifiedTestContext context, final BsonDocument operation, final int operationIndex,
            final OperationResult result) {
        context.getAssertionContext().push(ContextElement.ofCompletedOperation(operation, result, operationIndex));

        if (!operation.getBoolean("ignoreResultAndError", BsonBoolean.FALSE).getValue()) {
            if (operation.containsKey("expectResult")) {
                assertNull(result.getException(),
                        context.getAssertionContext().getMessage("The operation expects a result but an exception occurred"));
                context.getValueMatcher().assertValuesMatch(operation.get("expectResult"), result.getResult());
            } else if (operation.containsKey("expectError")) {
                assertNotNull(result.getException(),
                        context.getAssertionContext().getMessage("The operation expects an error but no exception was thrown"));
                context.getErrorMatcher().assertErrorsMatch(operation.getDocument("expectError"), result.getException());
            } else {
                assertNull(result.getException(),
                        context.getAssertionContext().getMessage("The operation expects no error but an exception occurred"));
            }
        }
        context.getAssertionContext().pop();
    }

    private OperationResult executeOperation(final UnifiedTestContext context, final BsonDocument operation, final int operationNum) {
        context.getAssertionContext().push(ContextElement.ofStartedOperation(operation, operationNum));
        String name = operation.getString("name").getValue();
        String object = operation.getString("object").getValue();
        try {
            switch (name) {
                case "createEntities":
                    return executeCreateEntities(operation);
                case "wait":
                    return executeWait(operation);
                case "waitForEvent":
                    return executeWaitForEvent(context, operation);
                case "waitForPrimaryChange":
                    return executeWaitPrimaryChange(context, operation);
                case "waitForThread":
                    return executeWaitForThread(context, operation);
                case "recordTopologyDescription":
                    return executeRecordTopologyDescription(operation);
                case "assertTopologyType":
                    return executeAssertTopologyType(context, operation);
                case "runOnThread":
                    return executeRunOnThread(context, operation);
                case "assertEventCount":
                    return executeAssertEventCount(context, operation);
                case "failPoint":
                    return executeFailPoint(operation);
                case "targetedFailPoint":
                    return executeTargetedFailPoint(operation);
                case "endSession":
                    return executeEndSession(operation);
                case "assertSessionDirty":
                    return executeAssertSessionDirty(operation);
                case "assertSessionNotDirty":
                    return executeAssertSessionNotDirty(operation);
                case "assertSessionPinned":
                    return executeAssertSessionPinned(operation);
                case "assertSessionUnpinned":
                    return executeAssertSessionUnpinned(operation);
                case "assertSameLsidOnLastTwoCommands":
                    return executeAssertSameLsidOnLastTwoCommands(operation);
                case "assertDifferentLsidOnLastTwoCommands":
                    return executeAssertDifferentLsidOnLastTwoCommands(operation);
                case "assertNumberConnectionsCheckedOut":
                    return executeAssertNumberConnectionsCheckedOut(context, operation);
                case "assertSessionTransactionState":
                    return executeAssertSessionTransactionState(operation);
                case "assertCollectionExists":
                    return executeAssertCollectionExists(operation);
                case "assertCollectionNotExists":
                    return executeAssertCollectionNotExists(operation);
                case "assertIndexExists":
                    return executeAssertIndexExists(operation);
                case "assertIndexNotExists":
                    return executeAssertIndexNotExists(operation);
                case "bulkWrite":
                    return crudHelper.executeBulkWrite(operation);
                case "insertOne":
                    return crudHelper.executeInsertOne(operation);
                case "insertMany":
                    return crudHelper.executeInsertMany(operation);
                case "updateOne":
                    return crudHelper.executeUpdateOne(operation);
                case "updateMany":
                    return crudHelper.executeUpdateMany(operation);
                case "replaceOne":
                    return crudHelper.executeReplaceOne(operation);
                case "deleteOne":
                    return crudHelper.executeDeleteOne(operation);
                case "deleteMany":
                    return crudHelper.executeDeleteMany(operation);
                case "aggregate":
                    return crudHelper.executeAggregate(operation);
                case "find":
                    if ("bucket".equals(object)){
                        return gridFSHelper.executeFind(operation);
                    }
                    return crudHelper.executeFind(operation);
                case "findOne":
                    return crudHelper.executeFindOne(operation);
                case "distinct":
                    return crudHelper.executeDistinct(operation);
                case "mapReduce":
                    return crudHelper.executeMapReduce(operation);
                case "countDocuments":
                    return crudHelper.executeCountDocuments(operation);
                case "estimatedDocumentCount":
                    return crudHelper.executeEstimatedDocumentCount(operation);
                case "findOneAndUpdate":
                    return crudHelper.executeFindOneAndUpdate(operation);
                case "findOneAndReplace":
                    return crudHelper.executeFindOneAndReplace(operation);
                case "findOneAndDelete":
                    return crudHelper.executeFindOneAndDelete(operation);
                case "listDatabases":
                    return crudHelper.executeListDatabases(operation);
                case "listDatabaseNames":
                    return crudHelper.executeListDatabaseNames(operation);
                case "listCollections":
                    return crudHelper.executeListCollections(operation);
                case "listCollectionNames":
                    return crudHelper.executeListCollectionNames(operation);
                case "listIndexes":
                    return crudHelper.executeListIndexes(operation);
                case "listIndexNames":
                    return crudHelper.executeListIndexNames(operation);
                case "dropCollection":
                    return crudHelper.executeDropCollection(operation);
                case "createCollection":
                    return crudHelper.executeCreateCollection(operation);
                case "modifyCollection":
                    return crudHelper.executeModifyCollection(operation);
                case "rename":
                    if ("bucket".equals(object)){
                        return gridFSHelper.executeRename(operation);
                    }
                    return crudHelper.executeRenameCollection(operation);
                case "createSearchIndex":
                    return crudHelper.executeCreateSearchIndex(operation);
                case "createSearchIndexes":
                    return crudHelper.executeCreateSearchIndexes(operation);
                case "updateSearchIndex":
                    return crudHelper.executeUpdateSearchIndex(operation);
                case "dropSearchIndex":
                    return crudHelper.executeDropSearchIndex(operation);
                case "listSearchIndexes":
                    return crudHelper.executeListSearchIndexes(operation);
                case "createIndex":
                    return crudHelper.executeCreateIndex(operation);
                case "dropIndex":
                    return crudHelper.executeDropIndex(operation);
                case "dropIndexes":
                    return crudHelper.executeDropIndexes(operation);
                case "startTransaction":
                    return crudHelper.executeStartTransaction(operation);
                case "commitTransaction":
                    return crudHelper.executeCommitTransaction(operation);
                case "abortTransaction":
                    return crudHelper.executeAbortTransaction(operation);
                case "withTransaction":
                    return crudHelper.executeWithTransaction(operation, (op, idx) -> assertOperationAndThrow(context, op, idx));
                case "createFindCursor":
                    return crudHelper.createFindCursor(operation);
                case "createChangeStream":
                    return crudHelper.createChangeStreamCursor(operation);
                case "clientBulkWrite":
                    return crudHelper.clientBulkWrite(operation);
                case "close":
                    return crudHelper.close(operation);
                case "iterateUntilDocumentOrError":
                    return crudHelper.executeIterateUntilDocumentOrError(operation);
                case "iterateOnce":
                    return crudHelper.executeIterateOnce(operation);
                case "delete":
                    return gridFSHelper.executeDelete(operation);
                case "drop":
                    return gridFSHelper.executeDrop(operation);
                case "download":
                    return gridFSHelper.executeDownload(operation);
                case "downloadByName":
                    return gridFSHelper.executeDownloadByName(operation);
                case "upload":
                    return gridFSHelper.executeUpload(operation);
                case "runCommand":
                    return crudHelper.executeRunCommand(operation);
                case "loop":
                    return loop(context, operation);
                case "createDataKey":
                    return clientEncryptionHelper.executeCreateDataKey(operation);
                case "addKeyAltName":
                    return clientEncryptionHelper.executeAddKeyAltName(operation);
                case "deleteKey":
                    return clientEncryptionHelper.executeDeleteKey(operation);
                case "removeKeyAltName":
                    return clientEncryptionHelper.executeRemoveKeyAltName(operation);
                case "getKey":
                    return clientEncryptionHelper.executeGetKey(operation);
                case "getKeys":
                    return clientEncryptionHelper.executeGetKeys(operation);
                case "getKeyByAltName":
                    return clientEncryptionHelper.executeGetKeyByAltName(operation);
                case "rewrapManyDataKey":
                    return clientEncryptionHelper.executeRewrapManyDataKey(operation);
                case "encrypt":
                    return clientEncryptionHelper.executeEncrypt(operation);
                case "decrypt":
                    return clientEncryptionHelper.executeDecrypt(operation);
                case "appendMetadata":
                    return crudHelper.executeUpdateClientMetadata(operation);
                default:
                    throw new UnsupportedOperationException("Unsupported test operation: " + name);
            }
        } finally {
            context.getAssertionContext().pop();
        }
    }

    private OperationResult loop(final UnifiedTestContext context, final BsonDocument operation) {
        BsonDocument arguments = operation.getDocument("arguments");

        int numIterations = 0;
        int numSuccessfulOperations = 0;
        boolean storeFailures = arguments.containsKey("storeFailuresAsEntity");
        boolean storeErrors = arguments.containsKey("storeErrorsAsEntity");
        BsonArray failureDescriptionDocuments = new BsonArray();
        BsonArray errorDescriptionDocuments = new BsonArray();

        while (!terminateLoop()) {
            BsonArray array = arguments.getArray("operations");
            for (int i = 0; i < array.size(); i++) {
                BsonValue cur = array.get(i);
                try {
                    assertOperation(context, cur.asDocument().clone(), i);
                    numSuccessfulOperations++;
                } catch (AssertionError e) {
                    if (storeFailures) {
                        failureDescriptionDocuments.add(createDocumentFromException(e));
                    } else if (storeErrors) {
                        errorDescriptionDocuments.add(createDocumentFromException(e));
                    } else {
                        throw e;
                    }
                    break;
                } catch (Exception e) {
                    if (storeErrors) {
                        errorDescriptionDocuments.add(createDocumentFromException(e));
                    } else if (storeFailures) {
                        failureDescriptionDocuments.add(createDocumentFromException(e));
                    } else {
                        throw e;
                    }
                    break;
                }
            }
            numIterations++;
        }

        if (arguments.containsKey("storeSuccessesAsEntity")) {
            entities.addSuccessCount(arguments.getString("storeSuccessesAsEntity").getValue(), numSuccessfulOperations);
        }
        if (arguments.containsKey("storeIterationsAsEntity")) {
            entities.addIterationCount(arguments.getString("storeIterationsAsEntity").getValue(), numIterations);
        }
        if (storeFailures) {
            entities.addFailureDocuments(arguments.getString("storeFailuresAsEntity").getValue(), failureDescriptionDocuments);
        }
        if (storeErrors) {
            entities.addErrorDocuments(arguments.getString("storeErrorsAsEntity").getValue(), errorDescriptionDocuments);
        }

        return OperationResult.NONE;
    }

    private BsonDocument createDocumentFromException(final Throwable throwable) {
        return new BsonDocument("error", new BsonString(throwable.toString()))
                .append("time", new BsonDouble(System.currentTimeMillis() / 1000.0));
    }

    protected boolean terminateLoop() {
        return true;
    }

    private OperationResult executeCreateEntities(final BsonDocument operation) {
        entities.init(operation.getDocument("arguments").getArray("entities"),
                startingClusterTime,
                false,
                this::createMongoClient,
                this::createGridFSBucket,
                this::createClientEncryption);
        return OperationResult.NONE;
    }

    private OperationResult executeWait(final BsonDocument operation) {
        try {
            Thread.sleep(operation.getDocument("arguments").getNumber("ms").longValue());
            return OperationResult.NONE;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private OperationResult executeWaitForEvent(final UnifiedTestContext context, final BsonDocument operation) {
        BsonDocument arguments = operation.getDocument("arguments");
        String clientId = arguments.getString("client").getValue();
        BsonDocument event = arguments.getDocument("event");
        String eventName = event.getFirstKey();
        int count = arguments.getNumber("count").intValue();

        switch (eventName) {
            case "serverDescriptionChangedEvent":
                context.getEventMatcher().waitForServerDescriptionChangedEvents(clientId, event, count,
                        entities.getServerListener(clientId));
                break;
            case "topologyDescriptionChangedEvent":
                context.getEventMatcher().waitForClusterDescriptionChangedEvents(clientId, event, count,
                        entities.getClusterListener(clientId));
                break;
            case "poolClearedEvent":
            case "poolReadyEvent":
            case "connectionCreatedEvent":
            case "connectionReadyEvent":
                context.getEventMatcher().waitForConnectionPoolEvents(clientId, event, count, entities.getConnectionPoolListener(clientId));
                break;
            case "serverHeartbeatStartedEvent":
            case "serverHeartbeatSucceededEvent":
            case "serverHeartbeatFailedEvent":
                context.getEventMatcher().waitForServerMonitorEvents(clientId, TestServerMonitorListener.eventType(eventName), event, count,
                        entities.getServerMonitorListener(clientId));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported event: " + eventName);
        }

        return OperationResult.NONE;
    }

    private OperationResult executeAssertEventCount(final UnifiedTestContext context, final BsonDocument operation) {
        BsonDocument arguments = operation.getDocument("arguments");
        String clientId = arguments.getString("client").getValue();
        BsonDocument event = arguments.getDocument("event");
        String eventName = event.getFirstKey();
        int count = arguments.getNumber("count").intValue();

        switch (eventName) {
            case "serverDescriptionChangedEvent":
                context.getEventMatcher().assertServerDescriptionChangeEventCount(clientId, event, count,
                        entities.getServerListener(clientId).getServerDescriptionChangedEvents());
                break;
            case "topologyDescriptionChangedEvent":
                context.getEventMatcher().assertClusterDescriptionChangeEventCount(clientId, event, count,
                        entities.getClusterListener(clientId).getClusterDescriptionChangedEvents());
                break;
            case "poolClearedEvent":
            case "poolReadyEvent":
                context.getEventMatcher().assertConnectionPoolEventCount(clientId, event, count,
                        entities.getConnectionPoolListener(clientId).getEvents());
                break;
            case "serverHeartbeatStartedEvent":
            case "serverHeartbeatSucceededEvent":
            case "serverHeartbeatFailedEvent":
                context.getEventMatcher().assertServerMonitorEventCount(clientId, TestServerMonitorListener.eventType(eventName), event, count,
                        entities.getServerMonitorListener(clientId));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported event: " + eventName);
        }

        return OperationResult.NONE;
    }

    private OperationResult executeWaitPrimaryChange(final UnifiedTestContext context, final BsonDocument operation) {
        context.getAssertionContext().push(ContextElement.ofWaitForPrimaryChange());
        BsonDocument arguments = operation.getDocument("arguments");
        MongoClient client = entities.getClient(arguments.getString("client").getValue());
        ClusterDescription priorClusterDescription =
                entities.getTopologyDescription(arguments.getString("priorTopologyDescription").getValue());
        ClusterDescription currentClusterDescription = client.getClusterDescription();
        long timeoutNanos =
                TimeUnit.NANOSECONDS.convert(arguments.getNumber("timeoutMS", new BsonInt32(10000)).longValue(), TimeUnit.MILLISECONDS);
        long startTime = System.nanoTime();
        while (primaryIsSame(priorClusterDescription, currentClusterDescription) || noPrimary(currentClusterDescription)) {
            if (System.nanoTime() - startTime > timeoutNanos) {
                fail(context.getAssertionContext().getMessage("Timed out waiting for primary change"));
            }
            try {
                //noinspection BusyWait
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            currentClusterDescription = client.getClusterDescription();
        }
        context.getAssertionContext().pop();
        return OperationResult.NONE;
    }

    private boolean noPrimary(final ClusterDescription clusterDescription) {
        return ReadPreference.primary().choose(clusterDescription).isEmpty();
    }

    private boolean primaryIsSame(final ClusterDescription priorClusterDescription, final ClusterDescription currentClusterDescription) {
        List<ServerDescription> priorPrimaries = ReadPreference.primary().choose(priorClusterDescription);
        List<ServerDescription> currentPrimaries = ReadPreference.primary().choose(currentClusterDescription);
        if (priorPrimaries.isEmpty() && currentPrimaries.isEmpty()) {
            return true;
        }
        if (priorPrimaries.size() == 1 && currentPrimaries.size() == 1) {
            return priorPrimaries.get(0).getAddress().equals(currentPrimaries.get(0).getAddress());
        } else {
            return false;
        }
    }

    private OperationResult executeWaitForThread(final UnifiedTestContext context, final BsonDocument operation) {
        BsonDocument arguments = operation.getDocument("arguments");
        String threadId = arguments.getString("thread").getValue();
        context.getAssertionContext().push(ContextElement.ofWaitForThread(threadId));
        List<Future<?>> tasks = entities.getThreadTasks(threadId);
        for (Future<?> task : tasks) {
            try {
                task.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException executionException) {
                try {
                    throw executionException.getCause();
                } catch (Throwable e) {
                    fail(context.getAssertionContext().getMessage(e.getMessage()));
                }
            } catch (TimeoutException e) {
                fail(context.getAssertionContext().getMessage(e.getMessage()));
            }
        }
        entities.clearThreadTasks(threadId);
        context.getAssertionContext().pop();
        return OperationResult.NONE;
    }

    private OperationResult executeRecordTopologyDescription(final BsonDocument operation) {
        BsonDocument arguments = operation.getDocument("arguments");
        ClusterDescription clusterDescription = entities.getClient(arguments.getString("client").getValue()).getClusterDescription();
        String topologyDescriptionId = arguments.getString("id").getValue();
        entities.addTopologyDescription(topologyDescriptionId, clusterDescription);
        return OperationResult.NONE;
    }

    private OperationResult executeAssertTopologyType(final UnifiedTestContext context, final BsonDocument operation) {
        BsonDocument arguments = operation.getDocument("arguments");
        ClusterDescription clusterDescription = entities.getTopologyDescription(arguments.getString("topologyDescription").getValue());
        String expectedTopologyType = arguments.getString("topologyType").getValue();

        context.getAssertionContext().push(ContextElement.ofTopologyType(expectedTopologyType));

        assertEquals(getClusterType(expectedTopologyType), clusterDescription.getType(),
                context.getAssertionContext().getMessage("Unexpected topology type"));

        context.getAssertionContext().pop();
        return OperationResult.NONE;
    }

    private ClusterType getClusterType(final String topologyType) {
        if (topologyType.equalsIgnoreCase("Sharded")) {
            return ClusterType.SHARDED;
        } else if (topologyType.equalsIgnoreCase("LoadBalanced")) {
            return ClusterType.LOAD_BALANCED;
        } else if (topologyType.startsWith("ReplicaSet")) {
            return ClusterType.REPLICA_SET;
        } else if (topologyType.equalsIgnoreCase("Unknown")) {
            return ClusterType.UNKNOWN;
        } else {
            throw new IllegalArgumentException("Unsupported topology type: " + topologyType);
        }
    }

    private OperationResult executeRunOnThread(final UnifiedTestContext context, final BsonDocument operation) {
        UnifiedTestContext newContext = new UnifiedTestContext();
        BsonDocument arguments = operation.getDocument("arguments");
        String threadId = arguments.getString("thread").getValue();
        ExecutorService thread = entities.getThread(threadId);
        Future<?> future = thread.submit(() -> assertOperation(newContext, arguments.getDocument("operation"), 0));
        entities.addThreadTask(threadId, future);
        return OperationResult.NONE;
    }

    private OperationResult executeFailPoint(final BsonDocument operation) {
        FailPoint failPoint = FailPoint.untargeted(operation, entities);
        failPoint.executeFailPoint();
        failPoints.add(failPoint);
        return OperationResult.NONE;
    }

    private OperationResult executeTargetedFailPoint(final BsonDocument operation) {
        FailPoint failPoint = FailPoint.targeted(operation, entities);
        failPoint.executeFailPoint();
        failPoints.add(failPoint);
        return OperationResult.NONE;
    }

    private OperationResult executeEndSession(final BsonDocument operation) {
        ClientSession session = entities.getSession(operation.getString("object").getValue());
        session.close();
        return OperationResult.NONE;
    }

    private OperationResult executeAssertSessionDirty(final BsonDocument operation) {
        return executeAssertSessionDirtiness(operation, true);
    }

    private OperationResult executeAssertSessionNotDirty(final BsonDocument operation) {
        return executeAssertSessionDirtiness(operation, false);
    }

    private OperationResult executeAssertSessionDirtiness(final BsonDocument operation, final boolean expected) {
        ClientSession session = entities.getSession(operation.getDocument("arguments").getString("session").getValue());
        assertNotNull(session.getServerSession());
        assertEquals(expected, session.getServerSession().isMarkedDirty());
        return OperationResult.NONE;
    }

    private OperationResult executeAssertSessionPinned(final BsonDocument operation) {
        return executeAssertSessionPinniness(operation, true);
    }

    private OperationResult executeAssertSessionUnpinned(final BsonDocument operation) {
        return executeAssertSessionPinniness(operation, false);
    }

    private OperationResult executeAssertSessionPinniness(final BsonDocument operation, final boolean expected) {
        ClientSession session = entities.getSession(operation.getDocument("arguments").getString("session").getValue());
        assertNotNull(session.getServerSession());
        assertEquals(expected, session.getPinnedServerAddress() != null);
        return OperationResult.NONE;
    }

    private OperationResult executeAssertNumberConnectionsCheckedOut(final UnifiedTestContext context, final BsonDocument operation) {
        TestConnectionPoolListener listener = entities.getConnectionPoolListener(
                operation.getDocument("arguments").getString("client").getValue());
        assertEquals(operation.getDocument("arguments").getNumber("connections").intValue(), listener.getNumConnectionsCheckedOut(),
                context.getAssertionContext().getMessage("Number of checked out connections must match expected"));
        return OperationResult.NONE;
    }

    private OperationResult executeAssertSameLsidOnLastTwoCommands(final BsonDocument operation) {
        return executeAssertLsidOnLastTwoCommands(operation, true);
    }

    private OperationResult executeAssertDifferentLsidOnLastTwoCommands(final BsonDocument operation) {
        return executeAssertLsidOnLastTwoCommands(operation, false);
    }

    private OperationResult executeAssertLsidOnLastTwoCommands(final BsonDocument operation, final boolean same) {
        TestCommandListener listener = entities.getClientCommandListener(
                operation.getDocument("arguments").getString("client").getValue());
        List<CommandEvent> events = lastTwoCommandEvents(listener);
        String eventsJson = listener.getCommandStartedEvents().stream()
                .map(e -> e.getCommand().toJson())
                .collect(Collectors.joining(", "));
        BsonDocument expected = ((CommandStartedEvent) events.get(0)).getCommand().getDocument("lsid");
        BsonDocument actual = ((CommandStartedEvent) events.get(1)).getCommand().getDocument("lsid");
        if (same) {
            assertEquals(expected, actual, eventsJson);
        } else {
            assertNotEquals(expected, actual, eventsJson);
        }
        return OperationResult.NONE;
    }

    private OperationResult executeAssertSessionTransactionState(final BsonDocument operation) {
        BsonDocument arguments = operation.getDocument("arguments");
        ClientSession session = entities.getSession(arguments.getString("session").getValue());
        String state = arguments.getString("state").getValue();
        switch (state) {
            case "starting":
            case "in_progress":
                assertTrue(session.hasActiveTransaction());
                break;
            default:
                throw new UnsupportedOperationException("Unsupported transaction state: " + state);
        }
        return OperationResult.NONE;
    }

    private OperationResult executeAssertCollectionExists(final BsonDocument operation) {
        assertTrue(collectionExists(operation));
        return OperationResult.NONE;
    }

    private OperationResult executeAssertCollectionNotExists(final BsonDocument operation) {
        assertFalse(collectionExists(operation));
        return OperationResult.NONE;
    }

    private boolean collectionExists(final BsonDocument operation) {
        BsonDocument arguments = operation.getDocument("arguments");
        String databaseName = arguments.getString("databaseName").getValue();
        String collectionName = arguments.getString("collectionName").getValue();
        return getMongoClient().getDatabase(databaseName)
                .listCollections().filter(Filters.eq("name", collectionName)).first() != null;
    }

    private OperationResult executeAssertIndexExists(final BsonDocument operation) {
        assertTrue(indexExists(operation));
        return OperationResult.NONE;
    }

    private OperationResult executeAssertIndexNotExists(final BsonDocument operation) {
        assertFalse(indexExists(operation));
        return OperationResult.NONE;
    }

    private boolean indexExists(final BsonDocument operation) {
        BsonDocument arguments = operation.getDocument("arguments");
        String databaseName = arguments.getString("databaseName").getValue();
        String collectionName = arguments.getString("collectionName").getValue();
        String indexName = arguments.getString("indexName").getValue();
        return getMongoClient().getDatabase(databaseName).getCollection(collectionName)
                .listIndexes(BsonDocument.class).into(new ArrayList<>()).stream()
                .anyMatch(document -> document.getString("name").getValue().equals(indexName));
    }

    private List<CommandEvent> lastTwoCommandEvents(final TestCommandListener listener) {
        List<CommandStartedEvent> events = listener.getCommandStartedEvents();
        assertTrue(events.size() >= 2);
        return new ArrayList<>(events.subList(events.size() - 2, events.size()));
    }

    private BsonDocument addInitialDataAndGetClusterTime() {
        for (BsonValue cur : initialData.getValues()) {
            BsonDocument curDataSet = cur.asDocument();
            CollectionHelper<BsonDocument> helper = new CollectionHelper<>(new BsonDocumentCodec(),
                    new MongoNamespace(curDataSet.getString("databaseName").getValue(),
                            curDataSet.getString("collectionName").getValue()));

            helper.create(WriteConcern.MAJORITY, curDataSet.getDocument("createOptions", new BsonDocument()));

            BsonArray documentsArray = curDataSet.getArray("documents", new BsonArray());
            if (!documentsArray.isEmpty()) {
                helper.insertDocuments(documentsArray.stream().map(BsonValue::asDocument).collect(toList()),
                        WriteConcern.MAJORITY);
            }
        }
        return getCurrentClusterTime();
    }

    protected void ignoreExtraCommandEvents(final boolean ignoreExtraEvents) {
        this.ignoreExtraEvents = ignoreExtraEvents;
    }

    protected void ignoreExtraEvents() {
        this.ignoreExtraEvents = true;
    }

    public enum Language {
        JAVA, KOTLIN
    }
}
