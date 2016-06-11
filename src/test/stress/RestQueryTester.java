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

public class RestQueryTester extends RestLoadTester {
    private String xmlDoc = "<doc>Where the future starts tomorrow!</doc>";
    private String jsonDoc = "{ \"key1\": \"value1\", \"key2\": [ \"value1\", \"value2\" ], \"key3\": 3, \"key4\": null, \"key5\": false }";
    private String ruleDef1 =
    "<rapi:rule xmlns:rapi=\"http://marklogic.com/rest-api\">"+
    "<rapi:description/>"+
    "<search xmlns=\"http://marklogic.com/appservices/search\">"+
    "<query>"+
        "<value-query>"+
            "<element ns=\"\" name=\"ruleCollection\"/>"+
            "<text>";
    private String ruleDef2 ="</text>"+
        "</value-query>"+
    "</query>"+
    "<options>"+
        "<search-option>unfiltered</search-option>"+
        "<transform-results apply=\"empty-snippet\"/>"+
        "<return-metrics>false</return-metrics>"+
    "</options>"+
    "</search>"+
    "<rapi:rule-metadata/>"+
    "</rapi:rule>";

    QueryTestData queryTestData;
    private String iterationName = "";

    public RestQueryTester(ConnectionData connData, QueryTestData queryData, String threadName, String iterationName) {
        super(connData, queryData.loadTestData, threadName);
        //System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "debug");
        this.queryTestData = queryData;
        this.iterationName = iterationName;
    }

    @Override
    public void init() throws Exception {
        setUniqueURI();
    }

    @Override
    protected void setUniqueURI() {
        randStr = randomString(16);
        dateStr = getDateString();
        uniqueURI = "/" + randStr + "/" + dateStr + "/" + threadName + "/" + iterationName + "/";
        dateStr += threadName;
    }

    public void verifyQuery(String test, int number, int count, String collection) {
        if (number != count) {
            String error = "ERROR (RestQueryTester.verifyQuery) query test ";
            if (test != null) error += test +" ";
            error += "failed (collection: " + collection + ") expected "
                    + number
                    + " contained " + count;
            System.err.println(error);
            System.err.println("Exiting...");
            alive = false;
        }
    }

    public void runQueries(int number, String collection) {
        message("Querying...");

        int count = getSession().search("future", collection, null);
        verifyQuery("future search", number,count, collection);

        count = getSession().kvSearch("key1", "value1", collection, null);
        verifyQuery("key-value search", number,count, collection);

// failing on frequency - 232 instead of 120
        count = getSession().values("key3", "coll:" + collection, null, null);
        verifyQuery("values search", number, count, collection);

        if (Math.random() > 0.5) {
            count = getSession().values("key3", "coll:" + collection, "avg", null);
            message("Average: " + count);
            verifyQuery("values average search", 3, count, collection);
        } else {
            count = getSession().values("key3", "coll:" + collection, "sum", null);
            message("Sum: " + count);
            verifyQuery("values sum search", 3 * number, count, collection);
        }

        message("Reading...");
        for (int pos = 0; pos < number; pos++) {
            String uri = collection + pos;
            String x = getSession().getDocument(uri + ".xml", null);
            String y = getSession().getDocument(uri + ".json", null);
            if (x == null || y == null) {
                // This can't happen
                System.err.println("Query document is null?");
            }
        }
    }

    public void runMatches(int number, String collection) {
        message("Matching...");

        String ruleDoc =
            "<ruleCollection>" + collection + "</ruleCollection>";
        int count = getSession().match(ruleDoc);
        verifyQuery("match", number, count, collection);
    }

    @Override
    /** If action is set to "query", sets up for queries with a random number of documents
     * between 100 and 125 then calls runQueries(number, collection).  If action is set to "match", sets up
     * for matches with a random number of saved rules between 20 and 25 then calls
     * runMatches(number, collection).
     */
    public void runTest() {
        try {
            startRun();
            init();
            message("========\n" +
                "Starting test with uniqueURI = " + uniqueURI + "\n" +
                queryTestData.toString() + "\n" +
                "========");

            for (int i = 0; i < testData.getNumOfLoops() && alive; i++) {
                String collection = uniqueURI + i + "/";

                String action = queryTestData.getAction();
                if ("query".equalsIgnoreCase(action)) {
                    int number = (int) Math.floor(Math.random() * 25.0) + 100;

                    message("Loading " + number  + " XML and JSON documents (collection: " + collection + ")");

                    for (int pos = 0; pos < number; pos++) {
                        String uri = collection + pos;
                        //message("Loading " + uri + ".xml");
                        getSession().putDocument(uri + ".xml", "application/xml", collection, xmlDoc.getBytes(), null);
                        //message("Loading " + uri + ".json");
                        getSession().putDocument(uri + ".json", "application/json", collection, jsonDoc.getBytes(), null);
                    }
                    runQueries(number, collection);

                } else if ("match".equalsIgnoreCase(action)) {
                    int number = (int) Math.floor(Math.random() * 5.0) + 20;
                    message("Loading " + number  + " rules (collection: " + collection + ")");

                    String ruleDef = ruleDef1 + collection + ruleDef2;

                    for (int pos = 0; pos < number; pos++) {
                        String ruleName = collection.substring(1) + pos;
                        ruleName = ruleName.replace("/", "-");
                        //message("Loading " + ruleName);
                        getSession().putRule(ruleName, ruleDef);
                    }

                    runMatches(number, collection);
            	}
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        alive = false;
        endRun();
    }
}
