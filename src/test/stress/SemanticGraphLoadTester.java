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
import java.util.List;
import com.marklogic.client.semantics.RDFMimeTypes;

public class SemanticGraphLoadTester extends RestLoadTester {
    private int batchSize = 50;

    public SemanticGraphLoadTester(ConnectionData connData, LoadTestData loadData,
            String threadName) {
        super(connData, loadData, threadName);
        System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "debug");
    }

    @Override
    protected void putDocument(String uri, String collection, byte[] content, Object transaction) throws Exception {
        if ( isTooBig(uri) ) return;
        String mimetype = getSession().getMimeType(uri);
        if ( mimetype != null ) {
            try {
                getSession().mergeGraph(uri, mimetype, new String(content, "UTF-8"), transaction);
            } catch (Exception e) {
                if ( isErrorDoc(uri) ) {
                    // expected errors, do nothing if they fail
                } else {
                    message("ERROR writing graph " + uri + " with mimetype " + mimetype +
                            (transaction != null ? " with" : " without") + " transaction");
                    alive = false;
                    throw e;
                }
            }
        }
    }

    private List<String> filterUris(List<String> uris) {
        List<String> validUris = new ArrayList<String>();
        for ( String uri : uris ) if ( isGraphDoc(uri) ) validUris.add(uri);
        return validUris;
    }

    private boolean isGraphDoc(String uri) {
        return
            ! isErrorDoc(uri) &&
            ! isTooBig(uri) &&
            (
                uri.endsWith(".ttl")  ||
                uri.endsWith(".nt")   ||
                uri.endsWith(".n3")   ||
                uri.endsWith(".rdf")  ||
                uri.endsWith(".json") ||
                uri.endsWith(".nq")   ||
                uri.endsWith(".trig") ||
                uri.endsWith(".xml")
            );
    }

    private boolean isTooBig(String uri) {
        return uri.endsWith("1500k_title_dbpedia.nt") ||
            uri.endsWith("1500k_title_dbpedia.ttl")   ||
            uri.endsWith("geo-zctas.n3")              ||
            uri.endsWith("geo-villages.n3")           ||
            uri.endsWith("geo-towns.n3")              ||
            uri.endsWith("dbpedia60k.nt")             ||
            uri.endsWith("bug31882.nt");
    }

    private boolean isErrorDoc(String uri) {
        return uri.endsWith("bug23846.xml")    ||
            uri.endsWith("error1.nq")    ||
            uri.endsWith("error1.nt")       ||
            uri.endsWith("error2.nq")       ||
            uri.endsWith("error3.ttl")      ||
            uri.endsWith("error4.rdf")      ||
            uri.endsWith("error5.xml")      ||
            uri.endsWith("error6.json")     ||
            uri.endsWith("escape-error.nt") ||
            uri.endsWith("mdata2.rdf")      ||
            uri.endsWith("non-iri.n3")      ||
            uri.endsWith("non-iri.nq")      ||
            uri.endsWith("non-iri.nt")      ||
            uri.endsWith("non-iri.trig")    ||
            uri.endsWith("non-iri.ttl")     ||
            uri.endsWith("non-iri.xml")     ||
            uri.endsWith("relative_error_mixed1.nq") ||
            uri.endsWith("relative_error_mixed2.nq") ||
            uri.endsWith("relative4.rdf")   ||
            uri.endsWith("semantics.n3")    ||
            uri.endsWith("semantics.xml")   ||
            uri.endsWith("mlcp-g1.nq")      ||
            uri.endsWith("mlcp-g2.nq")      ||
            uri.endsWith("mlcp-g3.nq");
    }

    @Override
    protected void verifyLoaded(int numLoaded, List<String> uris) throws Exception {
        if ( numLoaded == 0 ) return;
        List<String> validUris = filterUris(uris);
        numLoaded -= uris.size() - validUris.size();
        if ( numLoaded <= 0 ) return;
        boolean useSelect = Math.random() >= .5;
        for ( String uri : uris ) {
            if ( uri.endsWith(".trig") ) {
                // one can't read in trig format, just write, so use SELECT instead
                useSelect = true;
                break;
            }
        }
        try {
            long count = useSelect ?
                getSession().countGraphsViaSelect(null, validUris.toArray(new String[] {})) :
                getSession().countGraphsViaRead(null, validUris.toArray(new String[] {}));
            if ( count != numLoaded) {
                message("ERROR (SemanticGraphLoadTester.verifyLoaded) found " + count +
                        " loaded but should have found " + numLoaded + " of these URIs: " + validUris);
                System.err.println("Exiting...");
                alive = false;
            }
            getSession().deleteGraphs(null, validUris.toArray(new String[] {}));
        } catch (Exception e) {
            message("ERROR counting or deleting graphs " + validUris);
            e.printStackTrace();
        }

    }

    /** @return number of documents loaded */
    @Override
    protected int finishBatch(List<String> uris, boolean rollback, Object transaction) throws Exception {
        if (!rollback) {
            commitRestTransaction(transaction);
            return filterUris(uris).size(); // num uris loaded
        } else {
            rollbackRestTransaction(transaction);
            return 0; // none loaded
        }
    }
}

