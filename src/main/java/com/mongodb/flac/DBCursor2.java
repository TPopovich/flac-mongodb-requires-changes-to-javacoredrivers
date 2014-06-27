/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.flac;

import com.mongodb.*;
import org.bson.util.annotations.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * An iterator over database results.
 * Doing a <code>find()</code> query on a collection returns a
 * <code>DBCursor</code> thus
 * <p/>
 * <blockquote><pre>
 * DBCursor cursor = collection.find( query );
 * if( cursor.hasNext() )
 *     DBObject obj = cursor.next();
 * </pre></blockquote>
 * <p/>
 * <p><b>Warning:</b> Calling <code>toArray</code> or <code>length</code> on
 * a DBCursor will irrevocably turn it into an array.  This
 * means that, if the cursor was iterating over ten million results
 * (which it was lazily fetching from the database), suddenly there will
 * be a ten-million element array in memory.  Before converting to an array,
 * make sure that there are a reasonable number of results using
 * <code>skip()</code> and <code>limit()</code>.
 * <p>For example, to get an array of the 1000-1100th elements of a cursor, use
 * <p/>
 * <blockquote><pre>
 * List<DBObject> obj = collection.find( query ).skip( 1000 ).limit( 100 ).toArray();
 * </pre></blockquote>
 *
 * @dochub cursors
 */
@NotThreadSafe
class DBCursor2 extends com.mongodb.DBCursor {


    /**
     * Initializes a new database cursor
     *
     * @param collection collection to use
     * @param q          query to perform
     * @param k          keys to return from the query
     * @param preference the Read Preference for this query
     */
    public DBCursor2(DBCollection collection, DBObject q, DBObject k, ReadPreference preference) {
        super( collection,  q,  k,  preference);
    }

    /**
     * This is used just to create facade objects
     */
    public DBCursor2() {
       super();
    }


    // ----  result info ----
    // here we modify to make protected
    protected com.mongodb.QueryResultIterator _it = null;

}
