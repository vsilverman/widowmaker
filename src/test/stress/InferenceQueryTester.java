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


import java.util.Random;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.exceptions.XQueryException;

/**
 * ideas for inference test data:
 *  the query
 *  the expected results in rdf
 *  the expected results in jena
 *  checking the parsed results
 * 
 * ideas for the inference file:
 *  the query
 *  the expected results in turtle? rdf?
 */


public class
InferenceQueryTester
  extends XccLoadTester {
  static int VERIFY_RETRY_MAX = 5;
  static final Random randNum = new Random();
  private InferenceTestData inferenceTestData;
  int numTriplesLoaded = 0;

  public InferenceQueryTester(ConnectionData connData, InferenceTestData queryData,
          String threadName) {
    super(connData, queryData.loadTestData, threadName);
    System.out.println("****InferenceQueryTester constructor");
    inferenceTestData = queryData;
    inferenceTestData.updateQAHome();

    setTestName("InferenceQueryTester");

    ValidationManager validationMgr = StressManager.validationMgr;
    if (validationMgr != null)
      validationMgr.loadResults(inferenceTestData);
  }

  protected void
  loadContentFromDir(boolean rollback)
    throws InterruptedException
  {
    // we need to figure out how to just do one ballplayer here
    //
    // let's load the directory
    // each file contains a query and a result
    // the run command can then randomly grab a query, run it, and check the
    // results with the golden results
    
  }

  protected void
  runInferenceQuery(boolean rollback)
    throws InterruptedException, Exception
  {
    int queryPos = -1;
    while (queryPos < 0) {
      queryPos = randNum.nextInt() % inferenceTestData.getTotalQueries();
    }

    InferenceQuery q = inferenceTestData.getQueryObjAt(queryPos);
    String query = inferenceTestData.getQueryAt(queryPos);
    String results = inferenceTestData.getResultsForId(q.getIndex());

    // trying this here to see if it works - not the answer
    // setTransactionTimeout(3600);
    RequestOptions options = null;
    int timeoutVal = 0;
    if (inferenceTestData.getMinRequestTimeLimit() > 0) {
      int min = inferenceTestData.getMinRequestTimeLimit();
      int max = inferenceTestData.getMaxRequestTimeLimit();
      int range = max - min;
      // guard against the silly situation where max <= min
      if (range < 0)
        range = 2;

      timeoutVal = -1;
      while (timeoutVal < 0) {
        timeoutVal = randNum.nextInt() % range;
      }
      timeoutVal += min;
      options = new RequestOptions();
      options.setRequestTimeLimit(timeoutVal);
    }

    for (SessionHolder s : sessions) {
      String value = null;
      String verificationQuery = query;
      try {
        int index;
        if (q != null)
          index = q.getIndex();
        else
          index = queryPos;
        ResultsLogEntry logEntry = new ResultsLogEntry("Thread " + threadName + " query " + index);
        logEntry.startTimer();
        try {
          String log = "InferenceQueryTester Thread " + threadName + " query "
                  + index;
          s.runQuery("xdmp:log('" + log + "')");

          value = getValueFromResults(s.runQuery(verificationQuery, options));
        }
        catch (XQueryException x) {
          logEntry.stopTimer();
          if ((x.getCode().equals("XDMP-EXTIME") ||
            x.getCode().equals("SVC-EXTIME"))) {
            System.out.println("Query timed out:  trapped " + x.getCode());
            logEntry.setIdentifier("QUERY TIMEOUT:  " + x.getCode() + " Thread " + threadName + " query " + index + " request limit " + timeoutVal);
            logEntry.setPassFail(false);
            ResultsLogger logger =
                StressManager.getResultsLogger(inferenceTestData.getLogFileName());
            logger.logResult(logEntry);
            continue;
          } else if (x.getCode().equals("XDMP-INFFULL")) {
            // I know this is handled the same as above, but we need to understand
            // better when this happens, what it affects, and how we should adapt to it
            System.out.println("Query canceled:  trapped " + x.getCode());
            logEntry.setIdentifier("QUERY CANCELED:  " + x.getCode() + " Thread " + threadName + " query " + index);
            logEntry.setPassFail(false);
            ResultsLogger logger =
                StressManager.getResultsLogger(inferenceTestData.getLogFileName());
            logger.logResult(logEntry);
            continue;
          } else if ((x.getCode().equals("SVC-MEMORY") ||
              x.getCode().equals("XDMP-MEMORY"))) {
            System.out.println("Query canceled:  trapped " + x.getCode());
            logEntry.setIdentifier("QUERY CANCELED:  " + x.getCode() + " Thread " + threadName + " query " + index);
            logEntry.setPassFail(false);
            ResultsLogger logger =
                StressManager.getResultsLogger(inferenceTestData.getLogFileName());
            logger.logResult(logEntry);
            continue;
          } else {
            // let's log it, then re-throw it
            System.out.println("caught exception:  " + x.getCode());
            System.out.println("Query failed:  trapped " + x.getCode());
            logEntry.setIdentifier("QUERY FAILED:  " + x.getCode() + " Thread " + threadName + " query " + index);
            logEntry.setPassFail(false);
            ResultsLogger logger =
                StressManager.getResultsLogger(inferenceTestData.getLogFileName());
            logger.logResult(logEntry);
            throw x;
          }
        }
        logEntry.stopTimer();

        if (StressManager.validationMgr != null) {
          InferenceQueryValidator v = new InferenceQueryValidator(q.getIndex(),
                                        value);
          v.setLogFileName(inferenceTestData.getLogFileName());
          v.setLogEntry(logEntry);
          v.validateThis(q.getIndex(), value);
          StressManager.validationMgr.validateThis(v);
        }

      }
      catch (Exception e)
      {
        e.printStackTrace();
        System.err.println("query that failed:");
        System.err.println(verificationQuery);
      }
    }
  }

  public void
  runTest() {
    try {
      System.out.println("runTest:  starting test ");
      System.out.println(loadTestData.toString());
      init();
      System.out.println(uniqueURI);

      ResultsLogEntry entry = new ResultsLogEntry("thread " + threadName
                                      + " start InferenceQuery ");
      entry.startTimer();

      entry.stopTimer();
      entry.setPassFail(true);
      ResultsLogger logger =
          StressManager.getResultsLogger(inferenceTestData.getLogFileName());
      if (logger != null)
        logger.logResult(entry);

      // create a new one to log the completion
      entry = new ResultsLogEntry("thread " + threadName
                                      + " complete InferenceQuery ");
      entry.startTimer();

      for (int i = 0; i < loadTestData.getNumOfLoops() && alive; i++) {

        connect();

        // playing with telemetry here
        updateCounter(i);

        uriDelta = Integer.toString(i);
        runInferenceQuery(loadTestData.getRollback());

        if (alive)
          verifyIntervalAfterIteration(i+1);

        disconnect();

      }

      entry.stopTimer();
      entry.setPassFail(true);
      if (logger != null)
        logger.logResult(entry);

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("runTest:  exception stopped test ");
      System.out.println(loadTestData.toString());
    }

    updateCounter(0);
    setTestName("Finished");

    alive = false;
  }

  protected void
  verifyInterval()
    throws Exception {
  
    if (loadTestData.isInsertTime())
      return;

    int serverIndex = 0;

    for (SessionHolder s : sessions) {
      // TODO:  place holder for some form of validation
      String xqStr = "fn:count(fn:collection(\"<Athletics>\"))";
      String value = getValueFromResults(s.runQuery(xqStr, null));
      int numInDB = Integer.parseInt(value);
      if (numInDB != numTriplesLoaded) {
        String error = "ERROR Loaded " + numTriplesLoaded + "triples, graph "
                + uniqueURI + " contained " + numInDB;
        s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      } else {
        if (connData.servers.get(serverIndex).replicas.size() > 0) {
          String timeStamp = getValueFromResults(s.runQuery(
            "xdmp:request-timestamp()", null));
          StressManager.replicaValidationPool.addValidation(new ValidationData(
            connData, xqStr, value, timeStamp, serverIndex));

        }
      }
      if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG"))
        s.runQuery(xqStr);

      serverIndex++;
    }

  }

  private class VerificationTask implements Runnable {
    private String query;

    public VerificationTask(String query) {
      this.query = query;
    }

    public void
    run() {
      try {
        int retryMax = VERIFY_RETRY_MAX;
        if (inferenceTestData.loadTestData.isInsertTime())
          retryMax = 120;
        for (SessionHolder s : InferenceQueryTester.this.sessions) {
          int retry = 0;
          String value;
          while (true) {
            value = getValueFromResults(s.runQuery(query, null));
            if (value.equalsIgnoreCase("true")) break;
            if (++retry > retryMax) break;
            String error = "RETRY running " + query;
            // s.runQuery("xdmp:log.....");
            System.err.println(error);
            Thread.sleep(5000);
          }
          if (!value.equalsIgnoreCase("true")) {
            String error = "ERROR running " + query;
            // s.runQuery("xdmp:log.....");
            System.err.println(error);
            System.err.println("Exiting...");
            alive = false;
            Thread.sleep(1000);
            System.exit(1);
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


