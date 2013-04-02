/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package com.mongodb;

import org.mongodb.CreateCollectionOptions;
import org.mongodb.Document;
import org.mongodb.MongoConnector;
import org.mongodb.MongoNamespace;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.command.Create;
import org.mongodb.command.DropDatabase;
import org.mongodb.command.GetLastError;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.command.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.result.QueryResult;
import org.mongodb.serialization.Serializer;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.mongodb.DBObjects.toCommandResult;

@ThreadSafe
@SuppressWarnings("deprecation")
public class DB implements IDB {
    private final Mongo mongo;
    private final String name;
    private final ConcurrentHashMap<String, DBCollection> collectionCache =
            new ConcurrentHashMap<String, DBCollection>();
    private volatile ReadPreference readPreference;
    private volatile WriteConcern writeConcern;

    private final Bytes.OptionHolder optionHolder;

    private final Serializer<Document> documentSerializer;


    DB(final Mongo mongo, final String dbName, final Serializer<Document> documentSerializer) {
        this.mongo = mongo;
        this.name = dbName;
        this.documentSerializer = documentSerializer;
        this.optionHolder = new Bytes.OptionHolder(mongo.getOptionHolder());
    }

    /**
     * Gets the Mongo instance
     *
     * @return the mongo instance that this database was created from.
     */
    public Mongo getMongo() {
        return mongo;
    }

    public void setReadPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
    }

    public void setWriteConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    public ReadPreference getReadPreference() {
        return readPreference != null ? readPreference : mongo.getReadPreference();
    }

    public WriteConcern getWriteConcern() {
        return writeConcern != null ? writeConcern : mongo.getWriteConcern();
    }

    /**
     * Starts a new "consistent request". Following this call and until requestDone() is called, all db operations
     * should use the same underlying connection. This is useful to ensure that operations happen in a certain order
     * with predictable results.
     */
    public void requestStart() {
        mongo.requestStart();
    }

    /**
     * Ends the current "consistent request"
     */
    public void requestDone() {
        mongo.requestDone();
    }

    /**
     * ensure that a connection is assigned to the current "consistent request" (from primary pool, if connected to a
     * replica set)
     */
    public void requestEnsureConnection() {
        requestStart();
    }


    public DBCollection getCollection(final String name) {
        DBCollection collection = collectionCache.get(name);
        if (collection != null) {
            return collection;
        }

        collection = new DBCollection(name, this, documentSerializer);
        final DBCollection old = collectionCache.putIfAbsent(name, collection);
        return old != null ? old : collection;
    }

    /**
     * Drops this database. Removes all data on disk. Use with caution.
     *
     * @throws MongoException
     */
    public void dropDatabase() {
        executeCommand(new DropDatabase());
    }

    /**
     * Returns a collection matching a given string.
     *
     * @param s the name of the collection
     * @return the collection
     */
    public DBCollection getCollectionFromString(String s) {
        DBCollection foo = null;

        int idx = s.indexOf(".");
        while (idx >= 0) {
            final String b = s.substring(0, idx);
            s = s.substring(idx + 1);
            if (foo == null) {
                foo = getCollection(b);
            } else {
                foo = foo.getCollection(b);
            }
            idx = s.indexOf(".");
        }

        if (foo != null) {
            return foo.getCollection(s);
        }
        return getCollection(s);
    }

    public String getName() {
        return name;
    }

    /**
     * Returns a set containing the names of all collections in this database.
     *
     * @return the names of collections in this database
     * @throws MongoException
     */
    public Set<String> getCollectionNames() {
        final MongoNamespace namespacesCollection = new MongoNamespace(name, "system.namespaces");
        final MongoFind findAll = new MongoFind().readPreference(org.mongodb.ReadPreference.primary());
        final QueryResult<Document> query = getConnector().query(
                namespacesCollection,
                findAll,
                documentSerializer,
                documentSerializer
        );

        final HashSet<String> collections = new HashSet<String>();
        final int lengthOfDatabaseName = getName().length();
        for (final Document namespace : query.getResults()) {
            final String collectionName = (String) namespace.get("name");
            if (!collectionName.contains("$")) {
                final String collectionNameWithoutDatabasePrefix = collectionName.substring(lengthOfDatabaseName + 1);
                collections.add(collectionNameWithoutDatabasePrefix);
            }
        }
        return collections;
    }

    public DBCollection createCollection(final String collName, final DBObject options) {
        boolean capped = false;
        int sizeInBytes = 0;
        boolean autoIndex = true;
        int maxDocuments = 0;
        if (options.get("capped") != null) {
            capped = Boolean.valueOf(options.get("capped").toString());
        }
        if (options.get("size") != null) {
            sizeInBytes = ((Number) options.get("size")).intValue();
        }
        if (options.get("autoIndexId") != null) {
            autoIndex = (Boolean) options.get("autoIndexId");
        }
        if (options.get("max") != null) {
            maxDocuments = ((Number) options.get("max")).intValue();
        }
        final CreateCollectionOptions createCollectionOptions =
                new CreateCollectionOptions(collName, capped, sizeInBytes, autoIndex, maxDocuments);

        try {
            executeCommand(new Create(createCollectionOptions));
            return getCollection(collName);
        } catch (MongoCommandFailureException ex) {
            throw new MongoException(ex);
        }

    }

    public boolean authenticate(final String username, final char[] password) {
        return false;  // TODO: Implement authentication!!!!
    }

    /**
     * Executes a database command. This method constructs a simple DBObject using cmd as the field name and {@code
     * true} as its valu, and calls {@link DB#command(com.mongodb.DBObject) }
     *
     * @param cmd command to execute
     * @return result of command from the database
     * @throws MongoException
     * @mongodb.driver.manual tutorial/use-database-commands commands
     */
    public CommandResult command(final String cmd) {
        return command(new BasicDBObject(cmd, Boolean.TRUE));
    }

    /**
     * Executes a database command.
     *
     * @param cmd document representing the command to execute
     * @return result of command from the database
     * @throws MongoException
     * @mongodb.driver.manual tutorial/use-database-commands commands
     */
    public CommandResult command(final DBObject cmd) {
        final MongoCommand command = new MongoCommand(DBObjects.toDocument(cmd))
                .readPreference(getReadPreference().toNew());
        final org.mongodb.result.CommandResult baseCommandResult;
        try {
            baseCommandResult = executeCommand(command);
        } catch (MongoCommandFailureException ex) {
            throw new MongoException(ex);
        }

        return toCommandResult(cmd, new ServerAddress(baseCommandResult.getAddress()), baseCommandResult.getResponse());
    }

    /**
     * Executes a database command.
     *
     * @param cmd       dbobject representing the command to execute
     * @param options   query options to use
     * @param readPrefs ReadPreferences for this command (nodes selection is the biggest part of this)
     * @return result of command from the database
     * @throws MongoException
     * @mongodb.driver.manual tutorial/use-database-commands commands
     */
    public CommandResult command(final DBObject cmd, final int options, final ReadPreference readPrefs) {
        //        readPrefs = getCommandReadPreference(cmd, readPrefs);
        //        cmd = wrapCommand(cmd, readPrefs);
        //
        //        Iterator<DBObject> i =
        //                getCollection("$cmd").__find(cmd, new BasicDBObject(), 0, -1, 0, options, readPrefs ,
        //                                             DefaultDBDecoder.FACTORY.create(), encoder);
        //        if ( i == null || ! i.hasNext() )
        //            return null;
        //
        //        DBObject res = i.next();
        //        ServerAddress sa = (i instanceof Result) ? ((Result) i).getServerAddress() : null;
        //        CommandResult cr = new CommandResult(cmd, sa);
        //        cr.putAll( res );
        //        return cr;
        throw new UnsupportedOperationException();
    }

    /**
     * Gets another database on same server
     *
     * @param name name of the database
     * @return
     */
    public DB getSisterDB(final String name) {
        return mongo.getDB(name);
    }

    @Override
    public boolean collectionExists(final String collectionName) {
        final Set<String> collectionNames = getCollectionNames();
        for (final String name : collectionNames) {
            if (name.equalsIgnoreCase(collectionName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public CommandResult getLastError(final WriteConcern concern) {
        //TODO: this should be reflected somewhere in the new API?
        final GetLastError getLastErrorCommand = new GetLastError(concern.toNew());
        org.mongodb.result.CommandResult commandResult = executeCommand(getLastErrorCommand);
        commandResult = getLastErrorCommand.parseGetLastErrorResponse(commandResult);

        return new CommandResult(commandResult);
    }

    // ********* Missing functionality.  Sadly, also missing tests....

    @Override
    public CommandResult command(final DBObject cmd, final DBEncoder encoder) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult command(final DBObject cmd, final int options, final DBEncoder encoder) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult command(final DBObject cmd, final int options, final ReadPreference readPrefs,
                                 final DBEncoder encoder) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult command(final DBObject cmd, final int options) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult command(final String cmd, final int options) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult doEval(final String code, final Object... args) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public Object eval(final String code, final Object... args) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult getStats() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public void setReadOnly(final Boolean b) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult getLastError() {
        return getLastError(WriteConcern.ACKNOWLEDGED);
    }

    @Override
    public CommandResult getLastError(final int w, final int wtimeout, final boolean fsync) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public boolean isAuthenticated() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult authenticateCommand(final String username, final char[] password) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public WriteResult addUser(final String username, final char[] passwd) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public WriteResult addUser(final String username, final char[] passwd, final boolean readOnly) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public WriteResult removeUser(final String username) {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public CommandResult getPreviousError() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public void resetError() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public void forceError() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public void slaveOk() {
        throw new IllegalStateException("Not implemented yet!");
    }

    @Override
    public void addOption(final int option) {
        optionHolder.add(option);
    }

    @Override
    public void setOptions(final int options) {
        optionHolder.set(options);
    }

    @Override
    public void resetOptions() {
        optionHolder.reset();
    }

    @Override
    public int getOptions() {
        return optionHolder.get();
    }

    @Override
    public void cleanCursors(final boolean force) {
        throw new IllegalStateException("Not implemented yet!");
    }

    protected MongoConnector getConnector() {
        return getMongo().getConnector();
    }

    protected org.mongodb.result.CommandResult executeCommand(final MongoCommand commandOperation) {
        commandOperation.readPreferenceIfAbsent(getReadPreference().toNew());
        return new org.mongodb.result.CommandResult(getConnector().command(getName(), commandOperation, documentSerializer));

    }

    protected Bytes.OptionHolder getOptionHolder() {
        return optionHolder;
    }
}
