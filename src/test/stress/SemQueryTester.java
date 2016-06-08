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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class SemQueryTester extends XccLoadTester
    implements LoadableStressTester {
  
  private QueryTestData queryTestData;

  public SemQueryTester(ConnectionData connData, QueryTestData queryData,
      String threadName) {
    super(connData, queryData.loadTestData, threadName);
    queryTestData = queryData;
    queryTestData.updateCollection(uniqueURI);
    queryTestData.updateQAHome();

    setTestName("SemQueryTester");

  }
  
  public SemQueryTester() {
  }

  public void initialize(TestData testData, String threadName) {

    queryTestData = (QueryTestData)testData;
    super.initialize(testData, threadName);
    queryTestData.updateCollection(uniqueURI);
    queryTestData.updateQAHome();

    setTestName("SemQueryTester");

  }

  public TestData getTestDataInstance(String config)
      throws Exception {
    QueryTestData testData = new QueryTestData(config);
    return testData;
  }

  public String getTestType() {
    return "SemQueryTester";
  }

  /**
   * Checks the query against the loop counter to determine what
   * phase of the test we should be in. Checks always pass in
   * the current loop, but pass it in as (i+1), so this check
   * becomes one-based instead of zero-based. The intent is to
   * perform queries flagged as prior to loading to be performed
   * first, followed by any queries that result in loading data,
   * followed by queries that are evaluated after data loading has
   * taken place.
   */
  protected boolean skipThisQuery(Query query, int loop) {
  
    boolean bval = false;

    if (query == null)
      return true;

    if (loop == 1) {
      if (query.isPhase("pre-load"))
        bval = false;
      else
        bval = true;
    } else if (loop == 2) {
      if (query.isPhase("do-load"))
        bval = false;
      else
        bval = true;
    } else {
      if (query.isPhase("post-load"))
        bval = false;
      else
        bval = true;
    }

    return bval;
  }

  // overriding this because our triples turn into documents in the collection as well
  protected void verifyLoaded(String verificationQuery, int numLoaded,
      String curURI) throws Exception {
    if (loadTestData.isInsertTime()) 
      return;
    int serverIndex = 0;
    for (SessionHolder s : sessions) {
      String value = getValueFromResults(s.runQuery(verificationQuery, null));
      int numInDB = Integer.parseInt(value);
      // System.out.println("verifyLoaded:  total in collection " + curURI
      //                       + " is " + numInDB);
      if (numLoaded == 0) {
        String error = "ERROR could not find loaded URI : " + curURI;
        s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      } else { // query validated fine against server so add to replica
               // validation pool
        if (connData.servers.get(serverIndex).replicas.size() > 0) {
          // if we handle lag=0 testing, we don't send the timestamp
          String timeStamp = getValueFromResults(s.runQuery(
                          "xdmp:request-timestamp()", null));
          ValidationData vData = new ValidationData(
              connData, verificationQuery, value, timeStamp, serverIndex);
          // Waiting to see if we need more notification with lag testing
          // ReplicaValNotifier notifier = new ReplicaValNotifier(vData);
          StressManager.replicaValidationPool.addValidation(vData);
          // System.out.println(System.currentTimeMillis() + ": verifyLoaded: waitForComplete");
          // notifier.waitForComplete();
          // System.out.println(System.currentTimeMillis() + ": waitForComplete returned");
        }
      }
      serverIndex++;
    }
  }

  /**
   * We want to override this because the test now loads over and over again, though it's not
   * a problem
   */
  @Override
  protected void verifyInterval() throws Exception {
    if (loadTestData.isInsertTime()) 
      return;

    int serverIndex = 0;
    for (SessionHolder s : sessions) {
      String xqStr = "fn:count(fn:collection('" + uniqueURI + "'))";
      String value = getValueFromResults(s.runQuery(xqStr, null));
      int numInDB = Integer.parseInt(value);
      if (numInDB != numLoaded) {
        String error = "IGNORED Loaded " + numLoaded + " collection " + uniqueURI
            + " contained " + numInDB;
        s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
      } else { // query validated fine against server so add to replica
               // validation pool
        if (connData.servers.get(serverIndex).replicas.size() > 0) {
          String timeStamp = null;
          if (connData.servers.get(serverIndex).info.getLag() != 0) {
            timeStamp = getValueFromResults(s.runQuery(
                        "xdmp:request-timestamp()", null));
          }
          ReplicaValNotifier notifier = new ReplicaValNotifier();

          ValidationData vData = new ValidationData(
                  connData, xqStr, value, timeStamp, serverIndex, notifier);
          notifier.setValidationData(vData);
          StressManager.replicaValidationPool.addValidation(vData);

          notifier.waitForComplete();
        }
      }
      if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG"))
        s.runQuery(xqStr);

      serverIndex++;
    }
  }
  
  @Override
  protected void verifyIntervalAfterIteration(int loop) throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(queryTestData.concurrency);

    // let's just try this to see what happens:
    // loop == 0:  we're in pre-load
    // loop == 1:  we're doing load
    // loop > 1:  we're in post-load

    for (int i=0; i<queryTestData.repeat; i++) {
      for (Query query : queryTestData.queries) {
        int index = queryTestData.queries.indexOf(query);
        boolean skipQuery = skipThisQuery(query, loop);
        if (skipQuery) {
            ResultsLogEntry skipEntry =
              new ResultsLogEntry("Thread " + threadName + " query "
                        + index + " loop " + loop
                        + " repeat " + i
                        + " skipped");
              // skipEntry.stopTimer();
              skipEntry.setPassFail(true);
              VerificationTask task = new VerificationTask(query, i, loop, skipEntry);
              task.skip(true);
              exec.execute(task);
          } else {
            ResultsLogEntry logEntry =
              new ResultsLogEntry("Thread " + threadName + " query "
                    + index + " loop " + loop
                    + " repeat " + i);
            Query q = new Query(query.query.replaceAll("_LOOP_",
                    Integer.toString(loop)),query.language);
            Runnable task = new VerificationTask(q, i, loop, logEntry);
            exec.execute(task);
        }
      }
    }
    exec.shutdown();
    try {
      exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
      alive = false;
    }

  }
  
  private class VerificationTask implements Runnable {
    
    private Query query;
    private int queryID;
    private int loop;
    private boolean skip = false;
    private ResultsLogEntry logEntry;
    
    public VerificationTask(Query query, int queryID, int loop, ResultsLogEntry logEntry) {
      this.query = query;
      this.queryID = queryID;
      this.loop = loop;
      this.logEntry = logEntry;
    }    

    void skip(boolean bval) {
      skip = bval;
    }

    @Override
    public void run() {
      //no db replication support yet
      try {
        int retryMax = 0;
        if (queryTestData.loadTestData.isInsertTime())
          retryMax = 120;
        for (SessionHolder s : SemQueryTester.this.sessions) {
          int retry = 0;
          String value;

          logEntry.startTimer();

          if (!skip) {
          while (true) {
            // logEntry.startTimer();
            value = getValueFromResults(s.runQuery(query, null));
            if (value.equalsIgnoreCase("true")) break;
            if (++retry > retryMax) break;
            String error = "RETRY running " + query;
            s.runQuery("xdmp:log('" + error + "')");
            System.err.println(error);
            Thread.sleep(5000);
          }
          if (!value.equalsIgnoreCase("true")) {
            String error = "ERROR running " + query;
            s.runQuery("xdmp:log('" + error + "')");
            System.err.println(error);
            System.err.println("Exiting...");
            alive = false;
            Thread.sleep(1000);
            System.exit(1);
          } 
          logEntry.setPassFail(value.equalsIgnoreCase("true"));
          }
          logEntry.stopTimer();
          ResultsLogger logger = StressManager.getResultsLogger(loadTestData.getLogFileName());
          if (logger != null) {
            logger.logResult(logEntry);
          }
        }
      } catch (Throwable t) {
        t.printStackTrace();
        String error = "EXCEPTION running " + query;
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      }
    }    
  }
}
