package com.mongodb.flac;

import com.mongodb.*;
import com.mongodb.flac.DBCursorBasedOnQueryResultIterator;
import com.mongodb.util.JSON;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class DBCursorBasedOnQueryResultIteratorTest {

    @BeforeClass
    public static void initCollectionInitConstants() throws Exception {
        DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();
        dbCollectionSrc.drop();    // drop and recreate the pristine dbCollection for testing
        seedCollectionTwoSampleRecords(dbCollectionSrc);
    }

    /** fetch the DBCollection used for testing RedactedDBCollection */
    private static DBCollection getDbCollectionUsedForTesting() throws UnknownHostException {
        Mongo mongo = new Mongo();

        DB db = mongo.getDB("test");
        return db.getCollection("person3");
    }

    @Ignore
    @Test
    public void testComment() throws Exception {
        final ArrayList<DBObject> aggregationPipeline = new ArrayList<DBObject>();
        DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();

        final DBCursorBasedOnQueryResultIterator resultIterator = new DBCursorBasedOnQueryResultIterator(aggregationPipeline, dbCollectionSrc, null, null,
                ReadPreference.primary());
        resultIterator.comment("comment");
        resultIterator.next();
    }

    @Test
    public void testHasNext() throws Exception {
        final BasicDBObject query = null;
        final BasicDBObject keys = new BasicDBObject("_id", 1);
        final ArrayList<DBObject> aggregationPipeline = new ArrayList<DBObject>();

        DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();
        final DBCursorBasedOnQueryResultIterator resultIterator = new DBCursorBasedOnQueryResultIterator(aggregationPipeline, dbCollectionSrc, query, keys, ReadPreference.primary());

        final boolean hasNext = resultIterator.hasNext();
        assertEquals(true, hasNext);

    }

    @Test
    public void testMaxScan() throws Exception {

    }

    @Test
    public void testMax() throws Exception {

    }

    @Test
    public void testMin() throws Exception {

    }

    @Test
    public void testReturnKey() throws Exception {

    }

    @Test
    public void testShowDiskLoc() throws Exception {

    }

    @Test
    public void testCopy() throws Exception {

    }

    @Test
    public void testIterator() throws Exception {

    }

    @Test
    public void testSort() throws Exception {

        //final BasicDBObject query = new BasicDBObject();
        final BasicDBObject query = null;
        final BasicDBObject keys = new BasicDBObject("_id", 1);
        final ArrayList<DBObject> aggregationPipeline = new ArrayList<DBObject>();

        DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();
        final DBCursorBasedOnQueryResultIterator resultIterator = new DBCursorBasedOnQueryResultIterator(aggregationPipeline, dbCollectionSrc, query, keys, ReadPreference.primary());

        DBObject orderBy = new BasicDBObject("_id", 1);
        resultIterator.sort(orderBy);
        final DBObject o1 = resultIterator.next();

        final DBCursorBasedOnQueryResultIterator resultIterator2 = new DBCursorBasedOnQueryResultIterator(aggregationPipeline, dbCollectionSrc, query, keys, ReadPreference.primary());

        DBObject orderByBackwards = new BasicDBObject("_id", -1);
        resultIterator2.sort(orderByBackwards);
        final DBObject o2 = resultIterator.next();

        assertNotSame(o1.get("_id"), o2.get("_id"));

    }

    @Test
    public void testAddSpecial() throws Exception {

    }

    @Test
    public void testHint() throws Exception {

    }

    @Test
    public void testHint1() throws Exception {

    }

    @Test
    public void testMaxTime() throws Exception {

    }

    @Test
    public void testSnapshot() throws Exception {

    }

    @Test
    public void testExplain() throws Exception {

    }

    @Test
    public void testLimit() throws Exception {

    }

    @Test
    public void testBatchSize() throws Exception {

    }

    @Test
    public void testSkip() throws Exception {

    }

    @Test
    public void testGetCursorId() throws Exception {

    }

    @Test
    public void testClose() throws Exception {

    }

    @Test
    public void testSlaveOk() throws Exception {

    }

    @Test
    public void testAddOption() throws Exception {

    }

    @Test
    public void testSetOptions() throws Exception {

    }

    @Test
    public void testResetOptions() throws Exception {

    }

    @Test
    public void testGetOptions() throws Exception {

    }

    @Test
    public void test_checkType() throws Exception {

    }

    @Test
    public void testNumGetMores() throws Exception {

    }

    @Test
    public void testGetSizes() throws Exception {

    }

    @Test
    public void testNumSeen() throws Exception {

    }


    @Test
    public void testNext() throws Exception {

    }

    @Test
    public void testCurr() throws Exception {

    }

    @Test
    public void testRemove() throws Exception {

    }

    @Test
    public void test_fill() throws Exception {

    }

    @Test
    public void testLength() throws Exception {

    }

    @Test
    public void testToArray() throws Exception {

    }

    @Test
    public void testToArray1() throws Exception {

    }

    @Test
    public void testItcount() throws Exception {

    }

    @Test
    public void testCount() throws Exception {

    }

    @Test
    public void testOne() throws Exception {

    }

    @Test
    public void testSize() throws Exception {

    }

    @Test
    public void testGetKeysWanted() throws Exception {

    }

    @Test
    public void testGetQuery() throws Exception {

    }

    @Test
    public void testGetCollection() throws Exception {

    }

    @Test
    public void testGetServerAddress() throws Exception {

    }

    @Test
    public void testSetReadPreference() throws Exception {

    }

    @Test
    public void testGetReadPreference() throws Exception {

    }

    @Test
    public void testSetDecoderFactory() throws Exception {

    }

    @Test
    public void testGetDecoderFactory() throws Exception {

    }

    @Test
    public void testToString() throws Exception {

    }

    @Test
    public void testHasFinalizer() throws Exception {

    }

    @Test
    public void testHashCode() throws Exception {

    }

    @Test
    public void testEquals() throws Exception {

    }

    @Test
    public void testClone() throws Exception {

    }

    @Test
    public void testFinalize() throws Exception {

    }




    // Sample data
    // ==================  record #1 =========================
    //    {
    //    	"_id" : ObjectId("5375052930040f83a06f115a"),5a"),
    //    	"firstName" : "Sheldon",
    //    	"lastName" : "Humphrey",
    //    	"ssn" : {
    //    		"sl" : [
    //    			[
    //    				{
    //    					"c" : "TS"
    //    				}
    //    			],
    //    			[
    //    				{
    //    					"sci" : "G"
    //    				}
    //    			]
    //    		],
    //    		"value" : "354-61-8555"
    //    	},
    //    	"country" : {
    //    		"sl" : [
    //    			[
    //    				{
    //    					"c" : "S"
    //    				}
    //    			],
    //    			[
    //    				{
    //    					"sci" : "HCS"
    //    				}
    //    			]
    //    		],
    //    		"value" : "IRAQ"
    //    	},
    //    	"favorites" : {
    //    		"sl" : [
    //    			[
    //    				{
    //    					"c" : "S"
    //    				}
    //    			]
    //    		],
    //    		"cartoonCharacters" : [
    //    			"Diablo The Raven ",
    //    			"Rabbit",
    //    			"bar"
    //    		]
    //    	},
    //    	"foo" : "bar"
    //    }
    // ==================  record #2 =========================
    //    {
    //    	"_id" : ObjectId("5375052930040f83a06f1160"),60"),
    //    	"firstName" : "Alice",
    //    	"lastName" : "Fuentes",
    //    	"ssn" : {
    //    		"sl" : [
    //    			[
    //    				{
    //    					"c" : "TS"
    //    				}
    //    			],
    //    			[
    //    				{
    //    					"sci" : "SI"
    //    				},
    //    				{
    //    					"sci" : "TK"
    //    				}
    //    			]
    //    		],
    //    		"value" : "409-56-5309"
    //    	},
    //    	"country" : {
    //    		"sl" : [
    //    			[
    //    				{
    //    					"c" : "TS"
    //    				}
    //    			],
    //    			[
    //    				{
    //    					"sci" : "HCS"
    //    				},
    //    				{
    //    					"sci" : "G"
    //    				}
    //    			]
    //    		],
    //    		"value" : "UNITED STATES"
    //    	},
    //    	"favorites" : {
    //    		"sl" : [
    //    			[
    //    				{
    //    					"c" : "TS"
    //    				}
    //    			],
    //    			[
    //    				{
    //    					"sci" : "SI"
    //    				},
    //    				{
    //    					"sci" : "TK"
    //    				}
    //    			]
    //    		],
    //    		"cartoonCharacters" : [
    //    			"Tantor"
    //    		]
    //    	}
    //    }
    //============================================
    private static void seedCollectionTwoSampleRecords(DBCollection dbCollectionSrc) {
        dbCollectionSrc.insert(getRecordOne(), WriteConcern.NORMAL);
        dbCollectionSrc.insert(getRecordTwo(), WriteConcern.NORMAL);
    }


    private static DBObject getRecordOne() {
        if (0==1) return (DBObject) JSON.parse("{ \"_id\" : \"5375052930040f83a06f115a\", \"firstName\" : \"Sheldon\", \"quantity\": { \"sl\": [  [ { \"c\" : \"TS\" } ]  ], " +
                "\"value\": \"33\"}, \"lastName\" : \"Humphrey\", " +
                "\"ssn\" : { \"sl\" : [ [ { \"c\" : \"TS\" } ], [ { \"sci\" : \"G\" } ] ], \"value\" : \"354-61-8555\" }, \"country\" : { \"sl\" : [ [ { \"c\" : \"S\" } ], [ { \"sci\" : \"HCS\" } ] ], \"value\" : \"IRAQ\" }, \"favorites\" : { \"sl\" : [ [ { \"c\" : \"S\" } ] ], \"cartoonCharacters\" : [ \"Diablo The Raven \", \"Rabbit\", \"bar\" ] }, \"foo\" : \"bar\" }");
        if (0==1) return (DBObject) JSON.parse("{ \"_id\" : \"5375052930040f83a06f115a\", \"firstName\" : \"Sheldon\", \"tom\": \"33\", \"lastName\" : \"Humphrey\", " +
                "\"ssn\" : { \"sl\" : [ [ { \"c\" : \"TS\" } ], [ { \"sci\" : \"G\" } ] ], \"value\" : \"354-61-8555\" }, \"country\" : { \"sl\" : [ [ { \"c\" : \"S\" } ], [ { \"sci\" : \"HCS\" } ] ], \"value\" : \"IRAQ\" }, \"favorites\" : { \"sl\" : [ [ { \"c\" : \"S\" } ] ], \"cartoonCharacters\" : [ \"Diablo The Raven \", \"Rabbit\", \"bar\" ] }, \"foo\" : \"bar\" }");
        return (DBObject) JSON.parse("{ \"_id\" : \"5375052930040f83a06f115a\", \"firstName\" : \"Sheldon\", \"lastName\" : \"Humphrey\", " +
                "\"ssn\" : { \"sl\" : [ [ { \"c\" : \"TS\" } ], [ { \"sci\" : \"G\" } ] ], \"value\" : \"354-61-8555\" }, \"country\" : { \"sl\" : [ [ { \"c\" : \"S\" } ], [ { \"sci\" : \"HCS\" } ] ], \"value\" : \"IRAQ\" }, \"favorites\" : { \"sl\" : [ [ { \"c\" : \"S\" } ] ], \"cartoonCharacters\" : [ \"Diablo The Raven \", \"Rabbit\", \"bar\" ] }, \"foo\" : \"bar\" }");
    }

    private static DBObject getRecordTwo() {
        // "quantity": { "sl": [  [ { "c" : "TS" } ]  ], "value": "33"}
        if (0==1) return (DBObject) JSON.parse("{ \"_id\" : \"5375052930040f83a06f1160\", \"firstName\" : \"Alice\", \"quantity\": { \"sl\": [  [ { \"c\" : \"TS\" } ]  ], " +
                "\"value\": \"33\"}, \"lastName\" : \"Fuentes\", " +
                "\"ssn\" : { \"sl\" : [ [ { \"c\" : \"TS\" } ], [ { \"sci\" : \"SI\" }, { \"sci\" : \"TK\" } ] ], \"value\" : \"409-56-5309\" }, \"country\" : { \"sl\" : [ [ { \"c\" : \"TS\" } ], [ { \"sci\" : \"HCS\" }, { \"sci\" : \"G\" } ] ], \"value\" : \"UNITED STATES\" }, \"favorites\" : { \"sl\" : [ [ { \"c\" : \"TS\" } ], [ { \"sci\" : \"SI\" }, { \"sci\" : \"TK\" } ] ], \"cartoonCharacters\" : [ \"Tantor\" ] } }");
        if (0==1) return (DBObject) JSON.parse("{ \"_id\" : \"5375052930040f83a06f1160\", \"firstName\" : \"Alice\", \"tom\": \"33\", \"lastName\" : \"Fuentes\", " +
                "\"ssn\" : { \"sl\" : [ [ { \"c\" : \"TS\" } ], [ { \"sci\" : \"SI\" }, { \"sci\" : \"TK\" } ] ], \"value\" : \"409-56-5309\" }, \"country\" : { \"sl\" : [ [ { \"c\" : \"TS\" } ], [ { \"sci\" : \"HCS\" }, { \"sci\" : \"G\" } ] ], \"value\" : \"UNITED STATES\" }, \"favorites\" : { \"sl\" : [ [ { \"c\" : \"TS\" } ], [ { \"sci\" : \"SI\" }, { \"sci\" : \"TK\" } ] ], \"cartoonCharacters\" : [ \"Tantor\" ] } }");
        return (DBObject) JSON.parse("{ \"_id\" : \"5375052930040f83a06f1160\", \"firstName\" : \"Alice\", \"lastName\" : \"Fuentes\", " +
                "\"ssn\" : { \"sl\" : [ [ { \"c\" : \"TS\" } ], [ { \"sci\" : \"SI\" }, { \"sci\" : \"TK\" } ] ], \"value\" : \"409-56-5309\" }, \"country\" : { \"sl\" : [ [ { \"c\" : \"TS\" } ], [ { \"sci\" : \"HCS\" }, { \"sci\" : \"G\" } ] ], \"value\" : \"UNITED STATES\" }, \"favorites\" : { \"sl\" : [ [ { \"c\" : \"TS\" } ], [ { \"sci\" : \"SI\" }, { \"sci\" : \"TK\" } ] ], \"cartoonCharacters\" : [ \"Tantor\" ] } }");
    }


}