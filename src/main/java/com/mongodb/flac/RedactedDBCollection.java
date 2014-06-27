package com.mongodb.flac;

import com.mongodb.*;
import com.mongodb.flac.UserSecurityAttributesMap;
import com.mongodb.util.JSON;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
import static com.mongodb.QueryResultIterator.chooseBatchSize;
import static com.mongodb.WriteCommandResultHelper.getBulkWriteException;
import static com.mongodb.WriteCommandResultHelper.getBulkWriteResult;
import static com.mongodb.WriteCommandResultHelper.hasError;
import static com.mongodb.WriteRequest.Type.*;
import static com.mongodb.WriteRequest.Type.INSERT;
import static com.mongodb.WriteRequest.Type.REPLACE;
*/
import static java.lang.String.format;
import com.google.common.base.*;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.MongoTemplate;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.util.Assertions.isTrue;
import static org.bson.util.Assertions.notNull;

/**
 * RedactedDBCollection is a DBCollection that honors user specific FLAC security controls.  This allows
 * tight control on access to data, also known as Field Level Access Control.
 *
 * <p> The application can then use the DBCollection as they would use a normal DBCollection
 * for the most part.  Since the underlying find operations will be transformed into aggregation pipeline
 * there are a few minor restrictions.
 * </p>
 * <p> As a little code will show, you can now do something like this: </p>
 *
 * <p> <tt>redactedDBCollection.find(null, null).sort(orderBy).limit(100);</tt>
 * </p>
 */
@SuppressWarnings("deprecation")
//public class RedactedDBCollection { }
public class RedactedDBCollection extends com.mongodb.DBCollection {

    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(RedactedDBCollection.class);

    /**
     * The current users SecurityAttributes, which is a application specific mapping of security decls.
     */
    private UserSecurityAttributesMap userSecurityAttributes;
    public static final BasicDBObject EMPTY_OBJECT = new BasicDBObject(); // i.e. "{}";


    /**
     * Initializes a new safe collection.
     * Constructs a wrapper over a standard MongoDB collection {@link com.mongodb.DBCollection} for safe
     * access to a collection that
     * considers the information in the document honoring the
     * user specified FLAC "sl" (field level access control - security
     * level) field
     * before accessing documents from that collection. No operation is
     * actually performed on the database with this call,
     * we access it in a lazy manner.  *
     * <p>
     * This is similar to:
     *  <tt> RedactedDBCollection.fromCollection( db.getCollection("persons") , Map userSecurityAttributes ); </tt>
     * but is used to create the wrapper directly instead of using a builder pattern.
     *
     * Consider a simple use case. See the {@link com.mongodb.flac.docs.SampleApplicationDescription} docs that are also included in the kit for more information.
     * Given a DBCollection for the "persons" mongo collection
     * and a set of UserSecurityAttributes, which have meaning perhaps for a government application, e.g.
     * <pre><tt>
     *        clearance="TS"
     *        sci=[ "TK", "SI", "G", "HCS" ]
     *        countries=["US"]
     * </tt></pre>
     * might set user attributes.  These are specified in the userSecurityAttributes map.
     * <p/>
     * </p>
     * <p> This wrapper class will internally build up a deferred aggregationPipeline that is called
     *     eventually when the user actually fetches data.
     * </p>
     *
     * @param wrappedDBCollection the wrapped DB collection on which we operate
     * @param userSecurityAttributes      a Map of attributes, e.g.  clearance="TS", sci=[ "TK", "SI", "G", "HCS" ] etc
     *                            that provide the UserSecurityAttributes.  A detailed list of attributes might be:
     *                            <pre><tt>
     *                                   clearance="TS"
     *                                   sci=[ "TK", "SI", "G", "HCS" ]
     *                                   countries=["US"]
     *                            </tt></pre>
     */
    public RedactedDBCollection(DBCollection wrappedDBCollection, UserSecurityAttributesMap userSecurityAttributes) {
        super(Preconditions.checkNotNull(wrappedDBCollection, "wrappedDBCollection can't be null").getDB(), wrappedDBCollection.getName());
        this.userSecurityAttributes = userSecurityAttributes;
        this._wrapped = wrappedDBCollection;
        namespace = wrappedDBCollection.getFullName();

        userSecurityAttributes = Preconditions.checkNotNull(userSecurityAttributes, "userSecurityAttributes can't be null");

        //        UserSecurityAttributes attrCapcoUser = new UserSecurityAttributes(
        //                ((String) userSecurityAttributes.get(UserSecurityAttributes.Attributes.CLEARANCE.toString())),
        //                ((List<String>) userSecurityAttributes.get(UserSecurityAttributes.Attributes.SCI.toString())),
        //                ((List<String>) userSecurityAttributes.get(UserSecurityAttributes.Attributes.COUNTRIES.toString())));
    }

    /**
     * Builder to construct a wrapper for safe access a standard MongoDB collection {@link com.mongodb.DBCollection} that
     * considers the information in the document honoring the
     * user specified FLAC "sl" (field level access control - security
     * level) field
     * before accessing documents from that collection. No operation is
     * actually performed on the database with this call,
     * we access it in a lazy manner.
     *
     * <p/>
     * <p>
     * RedactedDBCollection.fromCollection( db.getCollection("persons") , Map userSecurityAttributes );
     * above is a simple use case.  Given a DBCollection for the "persons" mongo collection
     * and a set of UserSecurityAttributes, e.g.
     * <pre><tt>
     *        clearance="TS"
     *        sci=[ "TK", "SI", "G", "HCS" ]
     *        countries=["US"]
     * </tt></pre>
     * <p/>
     * </p>
     *
     * @param wrappedDBCollection the wrapped DB collection on which we operate
     * @param userSecurityAttributes      a Map of attributes, e.g.  clearance="TS", sci=[ "TK", "SI", "G", "HCS" ] etc
     *                            that provide the UserSecurityAttributes.  A detailed list of attributes might be:
     *                            <pre><tt>
     *                                   clearance="TS"
     *                                   sci=[ "TK", "SI", "G", "HCS" ]
     *                                   countries=["US"]
     *                            </tt></pre>
     */
    public static RedactedDBCollection fromCollection(DBCollection wrappedDBCollection, UserSecurityAttributesMap userSecurityAttributes) {
        return new RedactedDBCollection(wrappedDBCollection, userSecurityAttributes);
    }

    private DBCollection _wrapped;
    private UserSecurityAttributesMap _userSecurityString;
    private final String namespace;

    @Override                  // podpod
    public QueryResultIterator find(DBObject query, DBObject fields, int numToSkip, int batchSize, int limit, int options,
                             ReadPreference readPref, DBDecoder decoder) {
        if (willTrace()) {
            trace("RedactedDBCollection find: " + namespace + " " + JSON.serialize(query) + " fields " + JSON.serialize(safeDref(fields, EMPTY_OBJECT)));
        }
        final List<DBObject> aggregationPipeline = new ArrayList<DBObject>();
        final DBCursorBasedOnQueryResultIterator  dbCursorBasedOnQueryResultIterator = new DBCursorBasedOnQueryResultIterator(aggregationPipeline, /*DBCollection*/this, query, fields, readPref);
        QueryResultIterator resultIterator = (QueryResultIterator) dbCursorBasedOnQueryResultIterator.getAggregationQueryResultIterator();

        return resultIterator;
    }


    @Override               // podpod
    public QueryResultIterator find(DBObject query, DBObject fields, int numToSkip, int batchSize, int limit, int options,
                             ReadPreference readPref, DBDecoder decoder, DBEncoder encoder) {

        if (willTrace()) {
            trace("RedactedDBCollection find: " + namespace + " " + JSON.serialize(query) + " fields " + JSON.serialize(safeDref(fields, EMPTY_OBJECT)));
        }
        final List<DBObject> aggregationPipeline = new ArrayList<DBObject>();
        final DBCursorBasedOnQueryResultIterator  dbCursorBasedOnQueryResultIterator = new DBCursorBasedOnQueryResultIterator(aggregationPipeline, /*DBCollection*/this, query, fields, readPref);
        QueryResultIterator resultIterator = (QueryResultIterator) dbCursorBasedOnQueryResultIterator.getAggregationQueryResultIterator();

        return resultIterator;

    }




    /**
     * Queries for an object in this collection.
     *
     * @param query A document outlining the search query
     * @return an iterator over the results
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public com.mongodb.DBCursor find( DBObject query ){
        return find(query, null);
    }

    /**
     * Queries for an object in this collection.
     * <p>
     * An empty DBObject will match every document in the collection.
     * Regardless of fields specified, the _id fields are always returned.
     * </p>
     * <p>
     * An example that returns the "x" and "_id" fields for every document
     * in the collection that has an "x" field:
     * </p>
     * <pre>
     * {@code
     * BasicDBObject keys = new BasicDBObject();
     * keys.put("x", 1);
     *
     * DBCursor cursor = collection.find(new BasicDBObject(), keys);}
     * </pre>
     *
     * @param query object for which to search
     *  Restrictions
     * You cannot use $where in $match queries as part of the aggregation pipeline.
     * To use $text in the $match stage, the $match stage has to be the first stage of the pipeline,
     * but for security the redact will be first, so $text can not be utilized.
     * @param keys fields to return
     * @return a cursor to iterate over results
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public com.mongodb.DBCursor find( DBObject query , DBObject keys ){
        if (willTrace()) {
            trace("RedactedDBCollection find: " + namespace + " " + JSON.serialize(query) + " fields " + JSON.serialize(safeDref(keys, EMPTY_OBJECT)));
        }

        final boolean honorFLAC = true;
        if (! honorFLAC) {
            return getDbObjectsAndDontConsiderFLAC(query, keys);
        } else {
            return getDbObjectsAndHonorFLAC(query, keys);
        }
    }


    // This does not honor FLAC controls and just accesses the DB in the standard way
    private DBCursor getDbObjectsAndHonorFLAC(DBObject query, DBObject keys) {
        List<DBObject> pipeline = new ArrayList<DBObject>();

        final List<DBObject> pipelineSecure = prependSecurityRedactToPipeline(pipeline);


        return new DBCursorBasedOnQueryResultIterator(pipelineSecure, this._wrapped, query, keys, getReadPreference());

    }

    // This does not honor FLAC controls and just accesses the DB in the standard way
    private DBCursor getDbObjectsAndDontConsiderFLAC(DBObject query, DBObject keys) {
        return new DBCursor( this, query, keys, getReadPreference());
    }


    /**
     * Queries for all objects in this collection.
     *
     * @return a cursor which will iterate over every object
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public com.mongodb.DBCursor find(){
        return find(null, null);
    }

    /**
     * Returns a single object from this collection.
     *
     * @return the object found, or {@code null} if the collection is empty
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBObject findOne() {
        return findOne(new BasicDBObject());
    }

    /**
     * Returns a single object from this collection matching the query.
     * @param o the query object
     * @return the object found, or {@code null} if no such object exists
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBObject findOne( DBObject o ){
        return findOne( o, null, null, getReadPreference());
    }

    /**
     * Returns a single object from this collection matching the query.
     * @param o the query object
     * @param fields fields to return
     * @return the object found, or {@code null} if no such object exists
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBObject findOne( DBObject o, DBObject fields ) {
        return findOne( o, fields, null, getReadPreference());
    }

    /**
     * Returns a single object from this collection matching the query.
     * @param o the query object
     * @param fields fields to return
     * @param orderBy fields to order by
     * @return the object found, or {@code null} if no such object exists
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBObject findOne( DBObject o, DBObject fields, DBObject orderBy){
        return findOne(o, fields, orderBy, getReadPreference());
    }

    /**
     * Get a single document from collection.
     *
     * @param o        the selection criteria using query operators.
     * @param fields   specifies which fields MongoDB will return from the documents in the result set.
     * @param readPref {@link ReadPreference} to be used for this operation
     * @return A document that satisfies the query specified as the argument to this method, or {@code null} if no such object exists
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBObject findOne( DBObject o, DBObject fields, ReadPreference readPref ){
        return findOne(o, fields, null, readPref);
    }

    /**
     * Get a single document from collection.
     *
     * @param o        the selection criteria using query operators.
     * @param fields   specifies which projection MongoDB will return from the documents in the result set.
     * @param orderBy  A document whose fields specify the attributes on which to sort the result set.
     * @param readPref {@code ReadPreference} to be used for this operation
     * @return A document that satisfies the query specified as the argument to this method, or {@code null} if no such object exists
     * @throws MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    public DBObject findOne(DBObject o, DBObject fields, DBObject orderBy, ReadPreference readPref) {
        return findOne(o, fields, orderBy, readPref, 0, MILLISECONDS);
    }

    /**
     * Get a single document from collection.
     *
     * @param query       the selection criteria using query operators.
     * @param fields      specifies which projection MongoDB will return from the documents in the result set.
     * @param orderBy     A document whose fields specify the attributes on which to sort the result set.
     * @param readPref    {@code ReadPreference} to be used for this operation
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it
     * @param maxTimeUnit the unit that maxTime is specified in
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Query
     * @since 2.12.0
     */
    DBObject findOne(DBObject query, DBObject fields, DBObject orderBy, ReadPreference readPref,
                     long maxTime, TimeUnit maxTimeUnit) {

        final List<DBObject> aggregationPipeline = new ArrayList<DBObject>();
        final DBCursorBasedOnQueryResultIterator  dbCursorBasedOnQueryResultIterator = new DBCursorBasedOnQueryResultIterator(aggregationPipeline, /*DBCollection*/this, query, fields, readPref);
        dbCursorBasedOnQueryResultIterator.sort(orderBy);

        Iterator<DBObject> i = (QueryResultIterator) dbCursorBasedOnQueryResultIterator.getAggregationQueryResultIterator();

        DBObject obj = (i.hasNext() ? i.next() : null);
        if (obj != null && (fields != null && fields.keySet().size() > 0)) {
            obj.markAsPartialObject();
        }
        return obj;
    }

    /**
     * Do aggregation pipeline in a secure manner.
     * @param pipeline       List<DBObject> of operations for aggregation Pipeline
     * @param options
     * @param readPreference
     * @return QueryResultIterator or DBCursor (if the last part of pipeline had a $out)
     */
    public Cursor aggregate(final List<DBObject> pipeline, final AggregationOptions options,
                            final ReadPreference readPreference) {
        if (options == null) {
            throw new IllegalArgumentException("options can not be null");
        }
        if (pipeline == null) {
            throw new IllegalArgumentException("pipeline can not be null");
        }
        if (options == null) {
            throw new IllegalArgumentException("options can not be null");
        }

        final List<DBObject> pipelineSecure = prependSecurityRedactToPipeline(pipeline);
        return _wrapped.aggregate(pipelineSecure, options, readPreference);

    }

    /**
     * prepend the SecurityRedact Phrase To Pipeline
     *
     * @param pipeline original user specified Pipeline, note no check is made to see if the security
     *                 redact is already in the Pipeline.
     * @return new pipeline with SecurityRedact on front.
     */
    protected List<DBObject> prependSecurityRedactToPipeline(List<DBObject> pipeline) {
        String visibilityAttributesForUser = this.userSecurityAttributes.encodeFlacSecurityAttributes();
        return prependSecurityRedactToPipelineWorker(pipeline, visibilityAttributesForUser);
    }

    protected static List<DBObject> prependSecurityRedactToPipelineWorker(List<DBObject> pipeline, String visibilityAttributesForUser) {
        final DBObject redactCommandForPipeline = getRedactCommand(visibilityAttributesForUser);
        ArrayList<DBObject> newPipelineToReturn = new ArrayList<DBObject>();
        newPipelineToReturn.add(redactCommandForPipeline);
        newPipelineToReturn.addAll(pipeline);
        return newPipelineToReturn;
    }


    @Override
    @SuppressWarnings("unchecked")
    public List<Cursor> parallelScan(final ParallelScanOptions options) {
        return _wrapped.parallelScan(options);

    }

    @Override        // podpod
    public BulkWriteResult executeBulkWriteOperation(boolean ordered, List<WriteRequest> requests, WriteConcern writeConcern, DBEncoder encoder) {
        return _wrapped.executeBulkWriteOperation(ordered, requests, writeConcern, encoder);
    }


    //    @Override
    //    public BulkWriteResult executeBulkWriteOperation(final boolean ordered, final List<WriteRequest> writeRequests,
    //                                                     final WriteConcern writeConcern, DBEncoder encoder) {
    //        isTrue("no operations", !writeRequests.isEmpty());
    //
    //        return _wrapped.executeBulkWriteOperation(ordered, writeRequests,
    //                writeConcern, encoder);
    //    }

    @Override
    public WriteResult insert(List<DBObject> list, WriteConcern concern, DBEncoder encoder) {
        return _wrapped.insert(list, concern, encoder);
    }

    @Override
    public WriteResult remove(DBObject query, WriteConcern concern, DBEncoder encoder) {
        return _wrapped.remove(query, concern, encoder);
    }


    @Override
    public WriteResult update(DBObject query, DBObject o, boolean upsert, boolean multi, WriteConcern concern,
                              DBEncoder encoder) {
        if (o == null) {
            throw new IllegalArgumentException("update can not be null");
        }

        if (concern == null) {
            throw new IllegalArgumentException("Write concern can not be null");
        }
        if (willTrace()) {
            trace("update: " + namespace + " " + JSON.serialize(query) + " " + JSON.serialize(o));
        }
        return _wrapped.update(query, o, upsert, multi, concern, encoder);
    }


    @Override
    public void drop() {
        _wrapped.drop();
    }

    @Override
    public void createIndex(final DBObject keys, final DBObject options, DBEncoder encoder) {
        _wrapped.createIndex(keys, options, encoder);
    }


    private static final Logger TRACE_LOGGER = Logger.getLogger("com.mongodb.TRACE");
    private static final Level TRACE_LEVEL = Boolean.getBoolean("DB.TRACE") ? Level.INFO : Level.FINEST;

    private boolean willTrace() {
        return TRACE_LOGGER.isLoggable(TRACE_LEVEL);
    }

    private void trace(String s) {
        TRACE_LOGGER.log(TRACE_LEVEL, s);
    }

    private Logger getLogger() {
        return TRACE_LOGGER;
    }

    @Override
    public void doapply(DBObject o) {
        _wrapped.doapply(o);
    }


    // Util methods

    private Object safeDref(DBObject fields, DBObject def) {
        if (fields == null) return def;
        return fields;
    }

    private Object safeDref(String fields, String def) {
        if (fields == null) return def;
        return fields;
    }

    private Object safeDref(Number fields, Number def) {
        if (fields == null) return def;
        return fields;
    }



    /**
     * Add "$redact" mongodb command incantation to aggregate pipeline, note that we build a MATCH element
     * since that is where the $redact is nested, and append that to the pipeline.
     *
     * @param pipeline   the pipeline that will form the basis of the aggregate operation
     * @param criteria   the match criteria desired by user, if none pass in NULL
     * @param redact     the redact clause
     */
    private void addRedactionMatchToPipeline(List<DBObject> pipeline, DBObject criteria, DBObject redact) {
        pipeline.add(redact);         // redact is always added first
        if (criteria != null) {
            DBObject match = new BasicDBObject("$match", criteria);
            pipeline.add(match);
        }
    }

    /** Add "limit" for multi-object results to mongodb command incantation to pipeline */
    private void addLimitToPipeline(int recordLimit, List<DBObject> pipeline) {
        if (recordLimit <= 0)  return;        // TODO: should we also define a default page size for 0?

        DBObject limit = new BasicDBObject("$limit", recordLimit);

        pipeline.add(limit);
    }

    /** build the "$redact" mongodb command based on current FLAC user visibilityAttributesForUser setting */
    private static DBObject getRedactCommand(String visibilityAttributesForUser) {
        if (visibilityAttributesForUser == null || visibilityAttributesForUser.trim().length() == 0) {
            visibilityAttributesForUser = "[ ]";
        }
        String userSecurityExpression = String.format(com.mongodb.flac.RedactedDBCollectionConstants.getSecurityExpression(), visibilityAttributesForUser);
        logger.debug("**************** find() userSecurityExpression: " + userSecurityExpression);
        DBObject redactCommand = (DBObject) JSON.parse(userSecurityExpression);
        return new BasicDBObject("$redact", redactCommand);
    }


    //    /**
    //     * returns an iterator to the results of the find
    //     * @return the results of the aggregation
    //     */
    //    public <T extends PersistedDomainObject>  Iterable<DBObject> findRedactedResults(Class<T>  clz) {
    //        final AggregationOutput aggregate = this.aggregate(null, (ReadPreference) null);
    //
    //        final Iterable<DBObject> dbObjects = aggregate.results();
    //
    //        return dbObjects;
    //    }
    //
    //    private void test1234_pod() {
    //
    //        final DBCursor dbCursor = this.find();
    //
    //        final DBCursor cursor = this.find(null, null).sort(null).limit(100);
    //
    //        final AggregationOptions aggregationOptions = AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.CURSOR).build();
    //        final ArrayList<DBObject> pipeline = new ArrayList<DBObject>();
    //        final Cursor cursor1 = this.aggregate(pipeline, aggregationOptions, getReadPreference());
    //    }






}
