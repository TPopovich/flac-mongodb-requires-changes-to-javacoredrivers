package com.mongodb.flac;

import com.mongodb.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * DBCursorBasedOnQueryResultIterator is a DBCursor that can be based on either:
 *  <al>
 *     <li> DBCursor from a DBCollection , or on </li>
 *     <li> QueryResultIterator, e.g. from an aggregate output. </li>
 *  </al>
 */
class DBCursorBasedOnQueryResultIterator  extends com.mongodb.flac.DBCursor2 {

    private DBCollection dbCollectionSrc;
    private DBObject query;
    private DBObject fields;
    private ReadPreference readPreference;
    private List<DBObject> aggregationPipeline;

    private DBCollection dbCollection  = null;
    private QueryResultIterator queryResultIterator  = null;
    private boolean _ignoreMetaDataCalls = true;

    private Cursor aggregationPipelineOut;

//    /**
//     * Initializes a new database cursor
//     * @param queryResultIterator collection to use for iterating over
//     * @param query query to perform
//     * @param keys keys to return from the query
//     * @param readPreference the Read Preference for this query
//     */
//    public DBCursorBasedOnQueryResultIterator(QueryResultIterator queryResultIterator, DBCollection dbCollectionSrcResultSet,  DBObject query, DBObject keys, ReadPreference readPreference) {
//        super(null, query, keys, readPreference);
//        this.queryResultIterator = queryResultIterator;
//        this.dbCollectionSrcResultSet = dbCollectionSrcResultSet;
//        this.query = query;
//        this.keys = keys;
//        this.readPreference = readPreference;
//    }

    /**
     * Initializes a new database cursor  but this time based on a QueryResult, say from a
     * aggregation pipeline.  Mapping the details transparently to the end user.
     *
     * <p>
     *   The primary motivation for this is to enable Redacted DBCollection's to provide
     *   seamless interactions with the user.
     * </p>
     *
     * <p>
     *   Consider this series of activities.
     *   The user wants to protect a DBCollection with FLAC security but then intereact
     *   as if it were a usual DBCollection. For this to work, we need to do something like:
     *
     *   <tt><pre>
     *    RedactedDBCollection redactedDBCollection = new RedactedDBCollection(dbCollectionSrc, userSecurityAttributes);
     *
     *    then maybe do:
     *
     *    redactedDBCollection.find(query, keys).sort(orderBy).itcount()
     *    </pre></tt>
     *
     *   In general we want a seamless interaction with the user.
     * </p>
     *
     * @param aggregationPipeline  aggregation Pipeline iterating over
     * @param dbCollectionSrc  the source DBCollection that the aggregation Pipeline iterates over
     * @param query query to perform
     * @param fields projecting fields  to return from the query
     * @param readPreference the Read Preference for this query
     */
    public DBCursorBasedOnQueryResultIterator(List<DBObject> aggregationPipeline, DBCollection dbCollectionSrc,  DBObject query, DBObject fields, ReadPreference readPreference) {
        this.aggregationPipeline = aggregationPipeline;
        this.dbCollectionSrc = dbCollectionSrc;
        this.query = query;
        if (this.query == null) this.query = new BasicDBObject();
        this.fields = fields;
        if (this.fields == null) this.fields = new BasicDBObject("p8p8_placeholder", 0);
        this.readPreference = readPreference;
        super.setReadPreference(readPreference);
        if (query != null) { appendQueryToAggregationPipeline(this.query); }
        if (fields != null) { appendMatchToAggregationPipeline(this.fields); }

    }

    private void appendMatchToAggregationPipeline(DBObject keys) {
        if (dbObjectHasData(keys)) appendClauseToAggregationPipeline("$project", keys);
    }

    private void appendQueryToAggregationPipeline(DBObject criteria) {
        if (dbObjectHasData(criteria)) appendClauseToAggregationPipeline("$match", criteria);
    }



    private void appendLimitToAggregationPipeline(Object criteria) {
        if (dbObjectHasData(criteria)) appendClauseToAggregationPipeline("$limit", criteria);
    }

    private void appendSortToAggregationPipeline(Object orderBy) {
        if (dbObjectHasData(orderBy)) appendClauseToAggregationPipeline("$sort", orderBy);
    }

    private void appendSkipToAggregationPipeline(Object skipBy) {
        if (dbObjectHasData(skipBy)) appendClauseToAggregationPipeline("$skip", skipBy);
    }

    private boolean dbObjectHasData(DBObject dbObject) {
        if (dbObject != null) {
            if (dbObject instanceof BasicDBObject) {

                if (((BasicDBObject) dbObject).size() > 0) {
                    return true;
                }

            } else {
                return true;
            }
        }
        return false;
    }

    private boolean dbObjectHasData(Object object) {
        if (object != null) {
                return true;
        }
        return false;
    }

    private void appendClauseToAggregationPipeline(final String clauseKey, Object criteria) {
        if (this.aggregationPipeline == null) {
            throw new IllegalArgumentException("aggregationPipeline must not be null");
        }
        if (criteria != null) {
            DBObject match = new BasicDBObject(clauseKey, criteria);
            this.aggregationPipeline.add(match);
        }
    }



    /**
     * Initializes a new database cursor
     * @param collection collection to use
     * @param query query to perform
     * @param keys keys to return from the query
     * @param readPreference the Read Preference for this query
     */
    public DBCursorBasedOnQueryResultIterator(DBCollection collection, DBObject query, DBObject keys, ReadPreference readPreference) {
        super(collection, query, keys, readPreference);
    }

    @Override
    public com.mongodb.DBCursor comment(String comment) {
        if (dbCollection == null) { /* ignore */;  return this; }
        //if (dbCollection == null) throw new UnsupportedOperationException("only collection backed DBCursor can honor comment");
        return super.comment(comment);
    }

    @Override
    public com.mongodb.DBCursor maxScan(int max) {
        if (this.aggregationPipeline != null) { appendLimitToAggregationPipeline(new Integer(max));  return this; }
        return super.maxScan(max);
    }

    @Override
    public com.mongodb.DBCursor max(DBObject max) {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor max on index");
        return super.max(max);
    }

    @Override
    public com.mongodb.DBCursor min(DBObject min) {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor min on index");
        return super.min(min);
    }

    @Override
    public com.mongodb.DBCursor returnKey() {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor returnKey");
        return super.returnKey();
    }

    @Override
    public com.mongodb.DBCursor showDiskLoc() {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor showDiskLoc");
        return super.showDiskLoc();
    }

    @Override
    public com.mongodb.DBCursor copy() {
        return super.copy();
    }

    @Override
    public Iterator<DBObject> iterator() {
        return super.iterator();
    }

    @Override
    public com.mongodb.DBCursor sort(DBObject orderBy) {
        if (aggregationPipeline != null) { appendSortToAggregationPipeline(orderBy); return this; }
        return super.sort(orderBy);
    }

    @Override
    public com.mongodb.DBCursor addSpecial(String name, Object o) {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor addSpecial");
        return super.addSpecial(name, o);
    }

    @Override
    public com.mongodb.DBCursor hint(DBObject indexKeys) {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor hint");
        return super.hint(indexKeys);
    }

    @Override
    public com.mongodb.DBCursor hint(String indexName) {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor hint");
        return super.hint(indexName);
    }

    @Override
    public com.mongodb.DBCursor maxTime(long maxTime, TimeUnit timeUnit) {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor maxTime");
        return super.maxTime(maxTime, timeUnit);
    }

    @Override
    public com.mongodb.DBCursor snapshot() {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor snapshot");
        return super.snapshot();
    }

    @Override
    public DBObject explain() {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return new BasicDBObject(); }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor explain");
        return super.explain();
    }

    @Override
    public com.mongodb.DBCursor limit(int n) {
        if (this.aggregationPipeline != null) { appendLimitToAggregationPipeline(new Integer(n)); return this; }
        return super.limit(n);
    }

    @Override
    public com.mongodb.DBCursor batchSize(int n) {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        //if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor batchSize");
        return super.batchSize(n);
    }

    @Override
    public com.mongodb.DBCursor skip(int n) {
        if (aggregationPipeline != null) { appendSkipToAggregationPipeline(new Integer(n));  return this; }
        return super.skip(n);
    }

    @Override
    public long getCursorId() {
        if (aggregationPipeline != null) return getAggregationQueryResultIterator().getCursorId();
        return super.getCursorId();
    }

    @Override
    public void close() {
        if (aggregationPipeline != null) { getAggregationQueryResultIterator().close(); return; }
        super.close();
    }

    @Override
    public com.mongodb.DBCursor slaveOk() {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor slaveOk");
        return super.slaveOk();
    }

    @Override
    public com.mongodb.DBCursor addOption(int option) {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor addOption");
        return super.addOption(option);
    }

    @Override
    public com.mongodb.DBCursor setOptions(int options) {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor setOptions");
        return super.setOptions(options);
    }

    @Override
    public com.mongodb.DBCursor resetOptions() {
        if (aggregationPipeline != null && _ignoreMetaDataCalls == true) { /* ignore */;  return this; }
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor resetOptions");
        return super.resetOptions();
    }

    @Override
    public int getOptions() {
        if (aggregationPipeline != null) { return 0; }
        //if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor getOptions");
        return super.getOptions();
    }

    @Override
    protected void _checkType(CursorType type) {
        super._checkType(type);
    }

    @Override
    public int numGetMores() {
        return super.numGetMores();
    }

    @Override
    public List<Integer> getSizes() {
        return super.getSizes();
    }

    @Override
    public int numSeen() {
        return super.numSeen();
    }

    @Override
    public boolean hasNext() {
        if (aggregationPipeline != null) return getAggregationQueryResultIterator().hasNext();
        return super.hasNext();
    }

    @Override
    public DBObject next() {
        if (aggregationPipeline != null) return getAggregationQueryResultIterator().next();
        return super.next();
    }

    @Override
    public DBObject curr() {
        return super.curr();
    }

    @Override
    public void remove() {
        if (aggregationPipeline != null) getAggregationQueryResultIterator().remove();
        super.remove();
    }

    @Override
    protected void _fill(int n) {
        super._fill(n);
    }

    @Override
    /**
     * pulls back all items into an array and returns the number of objects.
     * Note: this can be resource intensive
     * @see #count()
     * @see #size()
     * @return the number of elements in the array
     * @throws MongoException
     */
    public int length() {
        //if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor length");
        return super.length();
    }

    @Override
    public List<DBObject> toArray() {
        return super.toArray();
    }

    @Override
    public List<DBObject> toArray(int max) {
        return super.toArray(max);
    }

    @Override
    public int itcount() {
        return super.itcount();
    }

    @Override
    public int count() {
        if (aggregationPipeline != null) { return super.itcount(); } // for aggregationPipeline we need to count as in itcount
        return super.count();
    }

    /**
     * @return the first matching document
     *
     * @since 2.12
     */
    @Override
    public DBObject one() {
        if (aggregationPipeline != null) {
            super._it = (QueryResultIterator) getAggregationQueryResultIterator(); // when an aggregation runs (w/o any $out set) it always creates a QueryResultIterator
        }

        return super.one();
    }

    /**
     * get the Aggregation QueryResultIterator which is a Cursor
     *
     * <p> NOTES: <br/>
     *      when an aggregation operation runs, without any $out set as we do in DBCursorBasedOnQueryResultIterator,
     *      it always creates a QueryResultIterator
     *
     * </p>
     *
     *  */
    public synchronized Cursor getAggregationQueryResultIterator() {
        if (aggregationPipelineOut == null) {

            this.aggregationPipeline = (this.aggregationPipeline == null) ? new ArrayList<DBObject>() : this.aggregationPipeline;
            final AggregationOptions aggregationOptions = AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.CURSOR).build();
            // the following call will return, in general,  a DBCursor or QueryResultIterator. It returns a
            // QueryResultIterator here, since the aggregation pipeline has no "$out"
            aggregationPipelineOut = this.dbCollectionSrc.aggregate(this.aggregationPipeline, aggregationOptions, getReadPreference());

            super._it = (QueryResultIterator) aggregationPipelineOut;
        }

        return aggregationPipelineOut;
    }

    @Override
    public int size() {
        if (aggregationPipeline != null) { return super.itcount(); } // for aggregationPipeline we need to count as in itcount
        return super.size();
    }

    @Override
    public DBObject getKeysWanted() {
        if (aggregationPipeline != null) { return this.fields; } // for aggregationPipeline we need to look at items stored here
        return super.getKeysWanted();
    }

    @Override
    public DBObject getQuery() {
        if (aggregationPipeline != null) { return this.query; } // for aggregationPipeline we need to look at items stored here
        return super.getQuery();
    }

    @Override
    public DBCollection getCollection() {
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor getCollection");
        return super.getCollection();
    }

    @Override
    public ServerAddress getServerAddress() {
        return super.getServerAddress();
    }

    @Override
    public com.mongodb.DBCursor setReadPreference(ReadPreference preference) {
        return super.setReadPreference(preference);
    }

    @Override
    public ReadPreference getReadPreference() {
        return super.getReadPreference();
    }

    @Override
    public com.mongodb.DBCursor setDecoderFactory(DBDecoderFactory fact) {
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor setDecoderFactory");
        return super.setDecoderFactory(fact);
    }

    @Override
    public DBDecoderFactory getDecoderFactory() {
        if (aggregationPipeline != null) throw new UnsupportedOperationException("only collection backed DBCursor can honor getDecoderFactory");
        return super.getDecoderFactory();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    protected boolean hasFinalizer() {
        return super.hasFinalizer();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }
}
