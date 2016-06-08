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


public class QueryTester extends XccLoadTester {
  
  private QueryTestData queryTestData;

  public QueryTester(ConnectionData connData, QueryTestData queryData,
      String threadName) {
    super(connData, queryData.loadTestData, threadName);
    queryTestData = queryData;
    queryTestData.updateCollection(uniqueURI);
    queryTestData.updateQAHome();

    setTestName("QueryTester");

  }
  
  @Override
  protected void verifyIntervalAfterIteration(int loop) throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(queryTestData.concurrency);
    for (int i=0; i<queryTestData.repeat; i++) {
      for (Query query : queryTestData.queries) {
        int index = queryTestData.queries.indexOf(query);
        boolean isLoadQuery = query.isPhase("do-load");
        if (isLoadQuery &&
              (queryTestData.loadTestData.loadLimit > 0) &&
              (i >= queryTestData.loadTestData.loadLimit)) {
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
        for (SessionHolder s : QueryTester.this.sessions) {
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
