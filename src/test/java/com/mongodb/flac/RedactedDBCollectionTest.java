package com.mongodb.flac;

import com.mongodb.*;
import com.mongodb.flac.UserSecurityAttributesMap;
import com.mongodb.util.JSON;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.Assert.*;


public class RedactedDBCollectionTest {

    @BeforeClass
    public static void initCollectionInitConstants() throws Exception {
        DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();
        dbCollectionSrc.drop();    // drop and recreate the pristine dbCollection for testing
        seedCollectionTwoSampleRecords(dbCollectionSrc);
        //System.out.println(dbCollectionSrc.find().itcount());              //pod
    }

    @Before
    public void initCollectionInitPerTest() throws Exception {
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


    // Test find( DBObject query , DBObject keys )
    @Test
    public void testFindTwoArgBasicDBObjectForThequery() throws Exception {

        final BasicDBObject query = new BasicDBObject();
        final BasicDBObject keys = new BasicDBObject();

        final DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();
        final UserSecurityAttributesMap userSecurityAttributes = new UserSecurityAttributesMap();
        final RedactedDBCollection redactedDBCollection = new RedactedDBCollection(dbCollectionSrc, userSecurityAttributes);
        final DBCursor dbObjects = redactedDBCollection.find(query, keys).sort(new BasicDBObject("firstName", -1));

        final boolean hasNext = dbObjects.hasNext();
        assertEquals(true, hasNext);
        final String expectedRec1 = "{ \"_id\" : \"5375052930040f83a06f115a\" , \"firstName\" : \"Sheldon\" , \"lastName\" : \"Humphrey\" , \"foo\" : \"bar\"}";

        final DBObject actual = dbObjects.next();
        compareJSON(expectedRec1, actual);

        final String expectedRec2 = "{ \"_id\" : \"5375052930040f83a06f1160\" , \"firstName\" : \"Alice\" , \"lastName\" : \"Fuentes\"}";
        compareJSON(expectedRec2, dbObjects.next());

        assertEquals(0, dbObjects.itcount());
    }

    // Test find( DBObject query , DBObject keys )
    @Test
    public void testFindTwoArgNullForThequery() throws Exception {

        final BasicDBObject query = null;
        final BasicDBObject keys = new BasicDBObject();

        final DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();
        final UserSecurityAttributesMap userSecurityAttributes = new UserSecurityAttributesMap();
        final RedactedDBCollection redactedDBCollection = new RedactedDBCollection(dbCollectionSrc, userSecurityAttributes);
        final DBCursor dbObjects = redactedDBCollection.find(query, keys);

        final boolean hasNext = dbObjects.hasNext();
        assertEquals(true, hasNext);
        final String expectedRec1 = "{ \"_id\" : \"5375052930040f83a06f115a\" , \"firstName\" : \"Sheldon\" , \"lastName\" : \"Humphrey\" , \"foo\" : \"bar\"}";

        final DBObject actual = dbObjects.next();
        compareJSON(expectedRec1, actual);

        final String expectedRec2 = "{ \"_id\" : \"5375052930040f83a06f1160\" , \"firstName\" : \"Alice\" , \"lastName\" : \"Fuentes\"}";
        compareJSON(expectedRec2, dbObjects.next());

        assertEquals(0, dbObjects.itcount());
    }

    // Test find( DBObject query , DBObject keys )
    @Test
    public void testFindTwoArgFindQuerySheldon() throws Exception {

        final BasicDBObject query = new BasicDBObject("firstName", "Sheldon");
        final BasicDBObject keys = new BasicDBObject();

        DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();
        final UserSecurityAttributesMap userSecurityAttributes = new UserSecurityAttributesMap();
        RedactedDBCollection redactedDBCollection = new RedactedDBCollection(dbCollectionSrc, userSecurityAttributes);
        final DBCursor dbObjects = redactedDBCollection.find(query, keys);

        final boolean hasNext = dbObjects.hasNext();
        assertEquals(true, hasNext);
        final String expectedRec1 = "{ \"_id\" : \"5375052930040f83a06f115a\" , \"firstName\" : \"Sheldon\" , \"lastName\" : \"Humphrey\" , \"foo\" : \"bar\"}";

        final DBObject actual = dbObjects.next();
        compareJSON(expectedRec1, actual);

        assertEquals(0, dbObjects.itcount());
    }

    @Test
    public void testFindTwoDocs() throws Exception {

        final BasicDBObject query = new BasicDBObject("firstName", "Sheldon");
        final BasicDBObject keys = new BasicDBObject();

        DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();
        // below we use the UserSecurityAttributesMapCapco that knows how to do our application logic of
        // c:TS also maps to c:S,  c:C, and  c:U
        final UserSecurityAttributesMap userSecurityAttributes = new UserSecurityAttributesMapCapco("c" , "TS");
        userSecurityAttributes.put("sci", "TK");
        RedactedDBCollection redactedDBCollection = new RedactedDBCollection(dbCollectionSrc, userSecurityAttributes);
        DBCursor dbObjectsCursor = redactedDBCollection.find(query, keys);
        DBObject orderBy = new BasicDBObject("_id", 1) ;
        final int itcount = dbObjectsCursor.itcount();
        assertEquals(1, itcount);
        assertEquals(false, dbObjectsCursor.hasNext());
        //reopen the cursor
        dbObjectsCursor = redactedDBCollection.find(query, keys);
        DBObject actual = dbObjectsCursor.next();
        final String expectedRec1 = "{ \"_id\" : \"5375052930040f83a06f115a\" , \"firstName\" : \"Sheldon\" , \"lastName\" : \"Humphrey\" , \"favorites\" : { \"sl\" : [ [ { \"c\" : \"S\"}]] , \"cartoonCharacters\" : [ \"Diablo The Raven \" , \"Rabbit\" , \"bar\"]} , \"foo\" : \"bar\"}";

        compareJSON(expectedRec1, actual);

        assertEquals(0, dbObjectsCursor.itcount());
    }

    // Test find( DBObject query , DBObject keys )
    @Test
    public void testFindTwoArgFindQuerySheldonTSClearance() throws Exception {

        final BasicDBObject query = new BasicDBObject("firstName", "Sheldon");
        final BasicDBObject keys = new BasicDBObject();

        DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();
        // below we use the UserSecurityAttributesMapCapco that knows how to do our application logic of
        // c:TS also maps to c:S,  c:C, and  c:U
        final UserSecurityAttributesMap userSecurityAttributes = new UserSecurityAttributesMapCapco("c" , "TS");
        RedactedDBCollection redactedDBCollection = new RedactedDBCollection(dbCollectionSrc, userSecurityAttributes);
        final DBCursor dbObjects = redactedDBCollection.find(query, keys);
        DBObject orderBy = new BasicDBObject("_id", 1) ;
        final int itcount = redactedDBCollection.find(query, keys).sort(orderBy).itcount();
        assertEquals(1, itcount);
        final boolean hasNext = dbObjects.hasNext();
        assertEquals(true, hasNext);
        final String expectedRec1 = "{ \"_id\" : \"5375052930040f83a06f115a\" , \"firstName\" : \"Sheldon\" , \"lastName\" : \"Humphrey\" , \"favorites\" : { \"sl\" : [ [ { \"c\" : \"S\"}]] , \"cartoonCharacters\" : [ \"Diablo The Raven \" , \"Rabbit\" , \"bar\"]} , \"foo\" : \"bar\"}";

        final DBObject actual = dbObjects.next();
        compareJSON(expectedRec1, actual);

        assertEquals(0, dbObjects.itcount());
    }

    @Test
    public void testFindTwoArgFindSheldonProjectJustFirstName() throws Exception {

        final BasicDBObject query = new BasicDBObject("firstName", "Sheldon");
        final BasicDBObject keys = new BasicDBObject("firstName", 1);

        DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();
        final UserSecurityAttributesMap userSecurityAttributes = new UserSecurityAttributesMap();
        RedactedDBCollection redactedDBCollection = new RedactedDBCollection(dbCollectionSrc, userSecurityAttributes);
        final DBCursor dbObjects = redactedDBCollection.find(query, keys);

        final boolean hasNext = dbObjects.hasNext();
        assertEquals(true, hasNext);
        final String expectedRec1 = "{ \"_id\" : \"5375052930040f83a06f115a\" , \"firstName\" : \"Sheldon\" }";

        final DBObject actual = dbObjects.next();
        compareJSON(expectedRec1, actual);

        assertEquals(0, dbObjects.itcount());
    }

    private void compareJSON(String json1, String json2)  {

        final DBObject j1 = (DBObject) JSON.parse(json1);
        final DBObject j2 = (DBObject) JSON.parse(json2);

        final HashSet<String> hashSetJ1 = new HashSet<String>(j1.keySet());
        final HashSet<String> hashSetJ2 = new HashSet<String>(j2.keySet());
        assertEquals(hashSetJ1, hashSetJ2);
        for (String k : hashSetJ1) {
            assertEquals(j1.get(k), j2.get(k));
        }
    }

    private void compareJSON(String json1, DBObject dbObject)  {

        final DBObject j1 = (DBObject) JSON.parse(json1);
        final DBObject j2 = dbObject;

        final HashSet<String> hashSetJ1 = new HashSet<String>(j1.keySet());
        final HashSet<String> hashSetJ2 = new HashSet<String>(j2.keySet());
        assertEquals(hashSetJ1, hashSetJ2);
        for (String k : hashSetJ1) {
            assertEquals(j1.get(k), j2.get(k));
        }
    }

    // Test find( DBObject query , DBObject keys )
    @Test
    public void testFindTwoArgItCount() throws Exception {
        final BasicDBObject query = null;
        final BasicDBObject keys = new BasicDBObject();

        DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();
        final UserSecurityAttributesMap userSecurityAttributes = new UserSecurityAttributesMap();
        RedactedDBCollection redactedDBCollection = new RedactedDBCollection(dbCollectionSrc, userSecurityAttributes);
        final DBCursor dbObjects = redactedDBCollection.find(query, keys);

        final boolean hasNext = dbObjects.hasNext();
        assertEquals(true, hasNext);
        assertEquals(2, dbObjects.itcount()); // should have 2 records  , as low level docs are protected only
    }


    @Test
    public void testFindOne() throws Exception {
        final BasicDBObject query = null;
        final BasicDBObject keys = new BasicDBObject();

        DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();
        final UserSecurityAttributesMap userSecurityAttributes = new UserSecurityAttributesMap();
        RedactedDBCollection redactedDBCollection = new RedactedDBCollection(dbCollectionSrc, userSecurityAttributes);
        final DBObject dbObject = redactedDBCollection.findOne(query, keys);

        assertNotNull(dbObject);
    }

    @Test
    public void testFindOne1() throws Exception {

    }

    @Test
    public void testInsert() throws Exception {
        DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();

        int c1 = (int) getDbCollectionUsedForTesting().count();

        final UserSecurityAttributesMap userSecurityAttributes = new UserSecurityAttributesMap();
        RedactedDBCollection redactedDBCollection = new RedactedDBCollection(dbCollectionSrc, userSecurityAttributes);
        final BasicDBObject o = new BasicDBObject();
        o.put("firstName", "Frank");
        final WriteResult writeResult = redactedDBCollection.insert(o, WriteConcern.NORMAL);

        assertEquals(0, writeResult.getN());            // write should succeed, but modify no other doc
        assertEquals(c1 + 1, getDbCollectionUsedForTesting().count());
    }

    @Test
    public void testRemove() throws Exception {
        DBCollection dbCollectionSrc = getDbCollectionUsedForTesting();

        int c1 = (int) getDbCollectionUsedForTesting().count();

        final UserSecurityAttributesMap userSecurityAttributes = new UserSecurityAttributesMap();
        RedactedDBCollection redactedDBCollection = new RedactedDBCollection(dbCollectionSrc, userSecurityAttributes);

        final BasicDBObject search = new BasicDBObject();
        search.put("firstName", "Sheldon");
        final WriteResult writeResult = redactedDBCollection.remove(search, WriteConcern.NORMAL);

        assertEquals(1, writeResult.getN());            // remove should succeed, and modify 1 doc
        assertEquals( c1-1,  getDbCollectionUsedForTesting().count() );
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