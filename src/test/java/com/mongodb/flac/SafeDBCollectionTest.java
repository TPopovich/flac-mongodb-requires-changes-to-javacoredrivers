package com.mongodb.flac;

import com.mongodb.*;
import com.mongodb.flac.UserSecurityAttributesMap;
import org.bson.types.BasicBSONList;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.util.*;

import static junit.framework.Assert.assertEquals;

public class SafeDBCollectionTest {

    @BeforeClass
    public static void classPresetConstants() throws FileNotFoundException {
        final String filename = "src/test/java/com/mongodb/flac/securityExpression.json";
        if (!(new File(filename).exists())) {
            throw new IllegalArgumentException("securityExpression.json file not properly setup: " + filename);
        }
        new com.mongodb.flac.RedactedDBCollectionConstants().setSecurityExpression(getSecurityExpressionFromFile(filename));

    }

    private static String getSecurityExpressionFromFile(final String filenameHoldingSecurityExpression) throws FileNotFoundException {

        final String content = new Scanner(new File(filenameHoldingSecurityExpression)).useDelimiter("\\Z").next();
        return content;

    }

    @Test
    public void testPrependSecurityRedactToPipeline() throws Exception {
        // should prepend a new
        final DBObject basicDBObject = new BasicDBObject();
        final DBObject basicDBObject2 = new BasicDBObject();
        final List<DBObject> pipeline = Arrays.asList(basicDBObject, basicDBObject2);
        final String visibilityAttributesForUser = "";
        final List<DBObject> actual = RedactedDBCollection.prependSecurityRedactToPipelineWorker(pipeline, visibilityAttributesForUser);
        org.junit.Assert.assertEquals(3, actual.size());
        org.junit.Assert.assertNotSame(basicDBObject, actual.get(0)); // element 0 should be our "src/test/java/com/mongodb/securityExpression.json" content
        org.junit.Assert.assertEquals(basicDBObject2, actual.get(1));

    }

    private static class DbCollectionHolder {

        public DB db;
        public DBCollection collection;

        public DbCollectionHolder(DB db, DBCollection collection) {
            this.db = db;
            this.collection = collection;
        }
    }

    private DbCollectionHolder initSimpleCollection(final String collectionName) throws UnknownHostException {

        Mongo mongo = new Mongo();

        DB db = mongo.getDB("test");
        DBCollection customersCollection = db.getCollection(collectionName);
        customersCollection.drop();

        DBObject address = new BasicDBObject("city", "NYC");
        address.put("street", "Broadway");

        DBObject addresses = new BasicDBObject();

        if (false) {
            BasicBSONList bsonList = new BasicBSONList();
            bsonList.putAll(addresses);
            addresses.putAll(bsonList);
        } else {
            addresses.putAll(address);
        }

        DBObject customer = new BasicDBObject("firstname", "Tom");
        customer.put("lastname", "Smith");
        customer.put("addresses", addresses);

        customersCollection.insert(customer);

        return new DbCollectionHolder(db, customersCollection);

    }

    @Test
    public void testFind() throws Exception {

    /*
     * @param userAttributes      a Map of attributes, e.g.  clearance="TS", sci=[ "TK", "SI", "G", "HCS" ] etc
     *                            that provide the UserSecurityAttributes.  A detailed list of attributes might be:
     *                            <pre><tt>
     *                                   clearance="TS"
     *                                   sci=[ "TK" ]
     *                                   countries=["US"]
     *                            </tt></pre>
     */

        DbCollectionHolder dbCollectionHolder = initSimpleCollection("ttt_customers");

        DB db = dbCollectionHolder.db;
        DBCollection customersCollection = dbCollectionHolder.collection;

        customersCollection.find();

        DBObject address = new BasicDBObject("city", "NYC");
        address.put("street", "Broadway");


        DBCollection persons = db.getCollection("persons");
        DBCollection wrappedDBCollection = persons;

        UserSecurityAttributesMap userAttributes = new UserSecurityAttributesMap();
        userAttributes.put("clearance", "TS");
        userAttributes.put("sci", Arrays.asList("TK", "SI", "G", "HCS"));
        userAttributes.put("countries", Arrays.asList("US"));

        RedactedDBCollection safeDBCollection = RedactedDBCollection.fromCollection(wrappedDBCollection, userAttributes);
        // test RedactedDBCollection

        DBObject customerQuery = new BasicDBObject("firstname", "Tom");

        safeDBCollection.find(customerQuery);

    }

    @Test
    public void testFind1() throws Exception {

    }

    @Test
    public void testAggregate() throws Exception {

    }

    @Test
    public void testParallelScan() throws Exception {

    }

    @Test
    public void testExecuteBulkWriteOperation() throws Exception {

    }

    @Test
    public void testInsert() throws Exception {

    }

    @Test
    public void testInsert1() throws Exception {

    }

    @Test
    public void testRemove() throws Exception {

    }

    @Test
    public void testRemove1() throws Exception {

    }

    @Test
    public void testUpdate() throws Exception {

    }

    @Test
    public void testDrop() throws Exception {

    }

    @Test
    public void testDoapply() throws Exception {

    }

    @Test
    public void testCreateIndex() throws Exception {

    }
}