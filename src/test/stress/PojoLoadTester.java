/*
 * Copyright 2003-2013 MarkLogic Corporation
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

package test.stress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;
import java.security.MessageDigest;
import java.math.BigInteger;

public class PojoLoadTester extends RestLoadTester {
    private List<Long> ids = new ArrayList<Long>();
    private List<Long> allIds = new ArrayList<Long>();
    private long countWithOverFiveChildren = 0;

    public PojoLoadTester(ConnectionData connData, LoadTestData loadData, String threadName) {
        super(connData, loadData, threadName);
        //System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "error");
    }

    public static long convertToLong(String uri) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] hashValue = md.digest(uri.getBytes());
        String myLong = new BigInteger(1, hashValue).toString();
        // I need to truncate to 15 digits to avoid loss of precision in db
        return new Long(myLong.substring(0, 14)).longValue();
    }

    public static Long[] convertToLongs(List<String> uris) throws Exception {
        Long[] longs = new Long[uris.size()];
        for ( int i=0; i < uris.size(); i++ ) {
            longs[i] = convertToLong(uris.get(i));
        }
        return longs;
    }

    @Override
    protected void putDocument(String uri, String collection, byte[] content, Object transaction) throws Exception {
        SamplePojo pojo = new SamplePojo();
        pojo.id = convertToLong(uri);
        pojo.shortData = pojo.id + getUriDelta() + " " + uri;
        pojo.integerData = ids.size();
        pojo.boolData = true;
        pojo.dateData = new GregorianCalendar();
        pojo.longData = new String(content);
        long numChildren = Math.abs(pojo.id % 10); // just a random number from 0-9
        if ( numChildren > 5 ) countWithOverFiveChildren++;
        SamplePojo copy1 = pojo;
        for ( int i=0; i < numChildren; i++ ) {
            SamplePojo copy2 = copy1.copy();
            copy2.childData = pojo;
            pojo = copy2;
        }
        getSession().putPojo(pojo, transaction, collection);
        ids.add(pojo.id);
    }

    @Override
    protected void verifyLoaded(int numLoaded, List<String> uris) throws Exception {
        Long[] ids = convertToLongs(uris);
        long count = getSession().countDocumentsViaPojo(null, ids);
        if ( count != numLoaded) {
            message("Exiting... ERROR (PojoLoadTester.verifyLoaded) found " + count +
                " loaded but should have found " + numLoaded + " of these ids: " + Arrays.asList(ids));
            alive = false;
        }
    }

    @Override
    /** Run queries after each loop.  For Pojo we need to do it this way because the data
     * from RestQueryTester doesn't work for pojo queries.  We use this method because it
     * allows us to execute once per configured loop.
     */
    protected void verifyIntervalAfterIteration(int loop, boolean rollback) throws Exception {
        if ( !rollback ) {
            long count = getSession().searchPojos(allIds, getCollection(), getUriDelta());
            if ( count != countWithOverFiveChildren ) {
                String error = "Exiting... ERROR (PojoLoadTester.verifyIntervalAfterIteration) found " + count +
                    " SamplePojo objects with over five children " +
                    "but should have found " + countWithOverFiveChildren;
                message(error);
                alive = false;
            }
        }
        // we're done with this loop, reset the counters
        countWithOverFiveChildren = 0;
        allIds = new ArrayList<Long>();
    }

    /** @return number of documents loaded */
    @Override
    protected int finishBatch(List<String> uris, boolean rollback, Object transaction) throws Exception {
        try {
            if (!rollback) {
                commitRestTransaction(transaction);
                return ids.size(); // num pojos loaded
            } else {
                rollbackRestTransaction(transaction);
                return 0; // none loaded
            }
        } finally {
            allIds.addAll(ids);
            ids = new ArrayList<Long>();
        }
    }
}
