/*
 * Copyright 2003-2016 MarkLogic Corporation
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

public class ExtensionLoadTester extends RestLoadTester {

    public ExtensionLoadTester(ConnectionData connData, LoadTestData loadData,
            String threadName) {
        super(connData, loadData, threadName);
        //System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "debug");
    }

    private void resetLists() {
    }

    @Override
    protected void putDocument(String uri, String collection, byte[] content, Object transaction) throws Exception {
        String contentType;
        if ( uri.endsWith("xml") ) contentType = "xml";
        else if ( uri.endsWith("json") ) contentType = "json";
        else if ( uri.endsWith("txt") ) contentType = "text";
        else contentType = "binary";
        getSession().putDocumentViaExtenstion(uri, contentType, content, collection, transaction);
    }

    @Override
    protected void verifyLoaded(int numLoaded, List<String> uris) throws Exception {
        long count = getSession().countDocumentsViaExtension(null, uris.toArray(new String[] {}));
        if ( count != numLoaded) {
            message("ERROR (ExtensionLoadTester.verifyLoaded) found " + count +
                " loaded but should have found " + numLoaded + " of these uris: " + uris);
            System.err.println("Exiting...");
            alive = false;
        }
    }

    /** @return number of documents loaded */
    @Override
    protected int finishBatch(List<String> uris, boolean rollback, Object transaction) throws Exception {
        resetLists();
        if (!rollback) {
            commitRestTransaction(transaction);
            return uris.size(); // num pojos loaded
        } else {
            rollbackRestTransaction(transaction);
            return 0; // none loaded
        }
    }
}
