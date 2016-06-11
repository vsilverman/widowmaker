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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Random;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.exceptions.RetryableXQueryException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.exceptions.XQueryException;

public class InferenceLoadTester extends XccLoadTester {
  static int VERIFY_RETRY_MAX = 5;
  private InferenceLoadTestData inferenceTestData;
  int numTriplesLoaded = 0;
  static final Random randNum = new Random();
  static SimpleDateFormat dateFormatter = null;
  static SimpleDateFormat dobFormatter = null;
  long dateRange;
  long dateSeed;

  public InferenceLoadTester(ConnectionData connData, InferenceLoadTestData queryData,
      String threadName) {
    super(connData, queryData.loadTestData, threadName);
    inferenceTestData = queryData;
    inferenceTestData.updateCollection(uniqueURI);
    inferenceTestData.updateQAHome();
    dateFormatter = new SimpleDateFormat("yyyyMMdd'_'HHmmss");
    dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    dobFormatter = new SimpleDateFormat("yyyy-MM-dd");

    setTestName("InferenceLoadTester");

    try {
    String tmpDate = "January 1, 1980";
    DateFormat format = new SimpleDateFormat("MMMM dd, yyyy");
    Date lowDate = format.parse(tmpDate);
    tmpDate = "December 31, 1995";
    Date highDate = format.parse(tmpDate);
    dateRange = highDate.getTime() - lowDate.getTime();
    dateSeed = lowDate.getTime();
    } catch (ParseException e) {
      e.printStackTrace();
      dateRange = 86400000L;
      dateSeed = 1L;
    }
  }

  String
  convertTeamToGraph(String team) {
    String str;
    // eliminate all spaces
    str = team.replaceAll(" ", "");
    // eliminate all dashes
    str = str.replaceAll("-", "");

    return str;
  }

  protected void loadTriplesFromDir(boolean rollback)
      throws InterruptedException {
    int queryPos = -1;
    while (queryPos < 0) {
      queryPos = randNum.nextInt() % inferenceTestData.getTotalQueries();
    }

    InferenceQuery q = inferenceTestData.getQueryObjAt(queryPos);
    String updateQuery = inferenceTestData.getQueryAt(queryPos);
    String verifyQuery = inferenceTestData.getVerifyQueryAt(queryPos);
    String resultsQuery = inferenceTestData.getResultsForId(q.getIndex());
    String dumpQuery = inferenceTestData.getDumpForId(q.getIndex());

    Date date = new Date();

    int ii = -1;

    while (ii < 0 ) {
      ii = randNum.nextInt() % inferenceTestData.teamnames.size();
    }
    String team = inferenceTestData.teamnames.get(ii);
    String graph = convertTeamToGraph(team);
    ii = -1;
    while (ii < 0 )
      ii = randNum.nextInt() % 999999;
    String id = Integer.toString(ii);
    String subject = "bb:" + randomString(8) + "_" + dateFormatter.format(date);
    ii = -1;
    while (ii < 0 )
      ii = randNum.nextInt() % 99999;
    String number = Integer.toString(ii);
    ii = -1;
    while (ii < 0 )
      ii = randNum.nextInt() % inferenceTestData.lastnames.size();
    String lastname = inferenceTestData.lastnames.get(ii);
    ii = -1;
    while (ii < 0 )
      ii = randNum.nextInt() % inferenceTestData.firstnames.size();
    String firstname = inferenceTestData.firstnames.get(ii);
    String teamrule = "bbr:" + graph;
    ii = -1;
    while (ii < 0 )
      ii = randNum.nextInt() % inferenceTestData.bats_choices.size();
    String bats = inferenceTestData.bats_choices.get(ii);
    ii = -1;
    while (ii < 0 )
      ii = randNum.nextInt() % inferenceTestData.throws_choices.size();
    String throws_with = inferenceTestData.throws_choices.get(ii);
    ii = -1;
    while (ii < 0 )
      ii = randNum.nextInt() % 70;
    String weight = Integer.toString(ii + 160);
    long ll = -1;
    while (ll < 0 )
      ll = randNum.nextLong() % dateRange;
    String dob = dobFormatter.format(new Date(ll+dateSeed));

    updateQuery = updateQuery.replaceAll("__TEAM_NAME__", team);
    updateQuery = updateQuery.replaceAll("__GRAPH__", graph);
    updateQuery = updateQuery.replaceAll("__SUBJECT__", subject);
    updateQuery = updateQuery.replaceAll("__ID__", id);
    updateQuery = updateQuery.replaceAll("__NUMBER__", number);
    updateQuery = updateQuery.replaceAll("__LASTNAME__", lastname);
    updateQuery = updateQuery.replaceAll("__FIRSTNAME__", firstname);
    updateQuery = updateQuery.replaceAll("__TEAM_RULE__", teamrule);
    updateQuery = updateQuery.replaceAll("__BATS__", bats);
    updateQuery = updateQuery.replaceAll("__THROWS__", throws_with);
    updateQuery = updateQuery.replaceAll("__WEIGHT__", weight);
    updateQuery = updateQuery.replaceAll("__DOB__", dob);

    verifyQuery = verifyQuery.replaceAll("__TEAM_NAME__", team);
    verifyQuery = verifyQuery.replaceAll("__GRAPH__", graph);
    verifyQuery = verifyQuery.replaceAll("__SUBJECT__", subject);
    verifyQuery = verifyQuery.replaceAll("__ID__", id);

    resultsQuery = resultsQuery.replaceAll("__TEAM_NAME__", team);
    resultsQuery = resultsQuery.replaceAll("__GRAPH__", graph);
    resultsQuery = resultsQuery.replaceAll("__SUBJECT__", subject);
    resultsQuery = resultsQuery.replaceAll("__ID__", id);

    if (dumpQuery != null) {
      dumpQuery = dumpQuery.replaceAll("__SUBJECT__", subject);
    }

    ResultsLogEntry logEntry = null;
    // we need to make the verification query here
    // to do so, we have to make the substitution strings locally so we can re-use them
    try {

       for (SessionHolder s : sessions) {
          long startTime = System.currentTimeMillis();
          logEntry = new ResultsLogEntry("thread " + threadName +
                                          " query " + queryPos);
          logEntry.startTimer();
          String value = getValueFromResults(s.runQuery(updateQuery, null));
          commitTransaction();
          logEntry.stopTimer();
          // System.out.println("InferenceLoadQuery:");
          // System.out.println(updateQuery);
          long elapsed = System.currentTimeMillis() - startTime;

          totalTime += elapsed;

          if (inferenceTestData.doVerify) {
            // this is verifying the count got added correctly. we need to both log this
            // separately and make sure it got recorded as an exception if this doesn't work
            boolean verifyCompleted = false;
            while (!verifyCompleted) {
              ResultsLogEntry verifyLogEntry = new ResultsLogEntry("thread " + threadName +
                                              " query " + queryPos);
              verifyLogEntry.startTimer();
              try {
                String verifyResponse = getValueFromResults(s.runQuery(verifyQuery, null));
                verifyCompleted = true;
                if (StressManager.validationMgr != null) {
                  InferenceLoadValidator v = new InferenceLoadValidator(q.getIndex(),
                                  value);
                  v.setLogFileName(inferenceTestData.getLogFileName());
                  v.setLogEntry(logEntry);
                  v.setVerifyResponse(verifyResponse);
                  v.setResultsQuery(resultsQuery);
                  StressManager.validationMgr.validateThis(v);
                }
              }
              catch (XQueryException x) {
                verifyLogEntry.stopTimer();
                if ((x.getCode().equals("SVC-CANCELED"))) {
                  System.out.println("Query returned canceled");
                  verifyLogEntry.setIdentifier("LOAD VERIFY " + x.getCode() + " Thread " + threadName
                            + " query " + queryPos);
                  verifyLogEntry.setPassFail(false);
                  ResultsLogger logger = 
                    StressManager.getResultsLogger(inferenceTestData.getLogFileName());
                  logger.logResult(verifyLogEntry);
                }
              }
            }
          } else {
            ResultsLogger logger =
                StressManager.getResultsLogger(inferenceTestData.getLogFileName());
            logger.logResult(logEntry);
          }
        }


        // throttles the work contributed by this thread
        long sleepMax = loadTestData.getMaxSleepTime();
        long sleepTime = -1;
        while (sleepTime < 0)
          sleepTime = randNum.nextLong() % sleepMax;

        Thread.sleep(sleepTime*1000L);

    }
    catch (XQueryException x) {
      logEntry.stopTimer();
      if ((x.getCode().equals("SVC-CANCELED"))) {
        System.out.println("Query returned canceled");
        logEntry.setIdentifier("QUERY CANCELED: " + x.getCode() + " LOAD Thread " + threadName
                  + " query " + queryPos);
        logEntry.setPassFail(false);
        ResultsLogger logger = 
          StressManager.getResultsLogger(inferenceTestData.getLogFileName());
        logger.logResult(logEntry);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      System.err.println("query that failed:");
      System.err.println(updateQuery);
    }

  }
  protected void loadContentFromDir(boolean rollback)
    throws InterruptedException {

  }

  @Override
  public void runTest() {
    try {
      System.out.println("runTest: starting test ");
      System.out.println(loadTestData.toString());
      init();
      System.out.println(uniqueURI);

      ResultsLogEntry entry = new ResultsLogEntry("thread " + threadName
                                      + " start InferenceLoad ");
      entry.startTimer();

      entry.stopTimer();
      entry.setPassFail(true);

      ResultsLogger logger = StressManager.getResultsLogger(inferenceTestData.getLogFileName());
      if (logger != null)
        logger.logResult(entry);

      // create a new one to log the completion
      entry = new ResultsLogEntry("thread " + threadName
                                      + " complete InferenceLoad ");
      entry.startTimer();

      for (int i = 0; i < loadTestData.getNumOfLoops() && alive; i++) {

        connect();

        updateCounter(i);

        if (isLoadDir) {
          uriDelta = Integer.toString(i);
          loadTriplesFromDir(loadTestData.getRollback());
        } else if (loadTestData.getGenerateQuery().length() != 0) {
          // loadTriplesFromQuery();
        }
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
    }

    updateCounter(0);
    setTestName("Finished");
    alive = false;

  }

  @Override 
  protected void verifyInterval() throws Exception {
    if (loadTestData.isInsertTime())
      return;
    int serverIndex = 0;
    for (SessionHolder s : sessions) {
      String xqStr = "fn:count(fn:collection('" + uniqueURI + "')/*:triples/*:triple)";
      String value = getValueFromResults(s.runQuery(xqStr, null));
      int numInDB = Integer.parseInt(value);
      if (numInDB != numTriplesLoaded) {
        String error = "ERROR Loaded " + numTriplesLoaded + "triples, graph " + uniqueURI
            + " contained " + numInDB;
        s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      } else { // query validated fine against server so add to replica
               // validation pool
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

    @Override
    public void run() {
      //no db replication support yet
      try {
        int retryMax = VERIFY_RETRY_MAX;
        if (inferenceTestData.loadTestData.isInsertTime())
          retryMax = 120;
        for (SessionHolder s : InferenceLoadTester.this.sessions) {        
          int retry = 0;
          String value;
          while (true) {
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
