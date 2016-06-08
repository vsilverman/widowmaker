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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.exceptions.RetryableXQueryException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.exceptions.XQueryException;
import com.marklogic.xcc.exceptions.QueryException;

public class SPARQLUpdateTester extends XccLoadTester {
  static int VERIFY_RETRY_MAX = 5;
  private QueryTestData queryTestData;
  int numTriplesLoaded = 0;
  public SPARQLUpdateTester(ConnectionData connData, QueryTestData queryData,
      String threadName) {
    super(connData, queryData.loadTestData, threadName);
    queryTestData = queryData;
    queryTestData.updateCollection(uniqueURI);
    queryTestData.updateQAHome();

    setTestName("SPARQLUpdateTester");
  }

  @Override
  protected void loadContentFromDir(boolean rollback)
      throws InterruptedException {
    File[] fileList = listFiles(loadTestData.getLoadDir());
    int curBatch = 0;
    int batchStart = 0;
    int retryCount = 0;
    String verificationQuery = "";
    int i = 0;
    while (i < fileList.length && alive) {
      boolean inserted = false;
      try {
        if (curBatch == 0) {
          beginTransaction();
          batchStart = i;
        }

        ContentCreateOptions options = null;
        if (!loadTestData.isBinaryTest()) {
          options = ContentCreateOptions.newXmlInstance();
          setTransactionTimeout(3600);
        } else
          options = ContentCreateOptions.newBinaryInstance();
        String[] collections = { uniqueURI };
        options.setCollections(collections);
        String curURI = uniqueURI + uriDelta + fileList[i].getName();

        Content content;
        if (loadTestData.isInsertTime()) {
          final Date d = new Date();
          SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
          df.setTimeZone(TimeZone.getTimeZone("GMT"));
          StringBuilder buf = new StringBuilder();

          try {
            FileReader fr = new FileReader(fileList[1]);
            BufferedReader br = new BufferedReader(fr);
            String sCurrentLine;
            int lineno = 1;
            while((sCurrentLine = br.readLine()) != null) {
              if (lineno == 2) {
                buf.append("<insert-time>");
                buf.append(df.format(d));
                buf.append("Z</insert-time>\n");
              }
              buf.append(sCurrentLine);
              buf.append("\n");
              lineno++;
            }
            br.close();
            fr.close();
          } catch (Exception e) {
          }
          content = ContentFactory.newContent(curURI, buf.toString(),
            options);

        } else {
          content = ContentFactory.newContent(curURI, fileList[i],
            options);
        }
        if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG")) {
          DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          Date date = new Date();
          System.out.println("Loading " + curURI + " at " + dateFormat.format(date));
        }
       for (SessionHolder s : sessions) {
          long startTime = System.currentTimeMillis();
          s.session.insertContent(content);
          long elapsed = System.currentTimeMillis() - startTime;
          if (elapsed > 60 * 1000) {
                 System.out.println("Took too long to load (" + elapsed + "): " + curURI);
          }
          totalTime += elapsed;
        }
        inserted = true;

        // if the current batch size is equal to the desired batch size,
        // or if this is the last file in the directory commit or rollback
        if (++curBatch >= loadTestData.getBatchSize()
            || i + 1 == fileList.length) {
          if (!rollback) {
            commitTransaction();
            numLoaded += curBatch;
            // verify all documents from this commit are loaded
            for (int pos = i; curBatch > 0; --pos) {
              --curBatch;
              curURI = uniqueURI + uriDelta + fileList[pos].getName();
              verificationQuery = "fn:count(fn:doc('" + curURI + "'))";
              verifyLoaded(verificationQuery, 1, curURI);
            }
          } else {
            rollbackTransaction();
            // verify all documents gone b/c rollback
            for (int pos = i; curBatch > 0; --pos) {
              --curBatch;
              curURI = uniqueURI + uriDelta + fileList[pos].getName();
              verificationQuery = "fn:count(fn:doc('" + curURI + "'))";
              verifyLoaded(verificationQuery, 0, curURI);
            }
          }
          // current batch has been added to total so 0 it out
          curBatch = 0;
          retryCount = 0;
        }
        // throttles the work contributed by this thread
        Thread.sleep(loadTestData.getMaxSleepTime());

        // check documents in db at interval
        // if multistmt have to check at batch end
/*        if (numLoaded % loadTestData.getCheckInterval() == 0 && curBatch == 0
            && !rollback)
          verifyInterval();
*/
        ++i;
      } catch (RetryableXQueryException e) {
        try {
          rollbackTransaction();
        } catch (Exception Ei) {
        }
        curBatch = 0;
        if (++retryCount > 25) {
          System.out.println("Retries exhausted");
          e.printStackTrace();
          ++i;
          retryCount = 0;
        } else {
          System.out.println("Retrying: " + retryCount);
          i = batchStart;
          Thread.sleep(1000);
        }
      } catch (ServerConnectionException e) {
        ++retryCount;
        if (retryCount < 100) {
          System.out.println("Retry for ServerConnectionException: " + e.getMessage() +
                             " count:" + retryCount);
          if (inserted) {
            i++;
            curBatch = 0;
          }
          Thread.sleep(10000);
          continue;
        }
        e.printStackTrace();
        try {
          rollbackTransaction();
        } catch (Exception Ei) {
        }
        ++i;
        curBatch = 0;
        retryCount = 0;
        totalTime = 0;
        String error = "ERROR could not load URI : " + uniqueURI;
        System.err.println(error);
        System.exit(1);
      } catch (XQueryException e) {
        // retry for XDMP-FORESTNID
        ++retryCount;
        if (e.getCode().equals("XDMP-FORESTNID") && retryCount < 100) {
          System.out.println("Retry for XDMP-FORESTNID: " + retryCount);
          if (inserted) {
            i++;
            curBatch = 0;
          }
          continue;
        } else if (e.getCode().equals("XDMP-DATABASEDISABLED") && retryCount < 100) {
          System.out.println("Retry for XDMP-DATABASEDISABLED: " + retryCount);
          if (inserted) {
            i++;
            curBatch = 0;
          }
          Thread.sleep(5000);
          continue;
        }
        e.printStackTrace();
        try {
          rollbackTransaction();
        } catch (Exception Ei) {
        }
        ++i;
        curBatch = 0;
        retryCount = 0;
        totalTime = 0;
        String error = "ERROR could not load URI : " + uniqueURI;
        System.err.println(error);
        System.exit(1);
      } catch (Throwable e) {
        // need to do something about exceptions and multistmt
        e.printStackTrace();
        try {
          rollbackTransaction();
        } catch (Exception Ei) {
        }
        ++i;
        curBatch = 0;
        retryCount = 0;
        totalTime = 0;
        String error = "ERROR could not load URI : " + uniqueURI;
        System.err.println(error);
        System.exit(1);
      }
    }
  }
  protected void loadTriplesFromDir(boolean rollback)
    throws InterruptedException, Exception {
    loadContentFromDir(rollback);
    for (SessionHolder s : sessions) {
      String verificationQuery = "fn:count(fn:collection('" + uniqueURI + "')/*:triples/*:triple)";
      String value = getValueFromResults(s.runQuery(verificationQuery, null));
      numTriplesLoaded = Integer.parseInt(value);
      break;
    }
  }

  @Override
  public void runTest() {
    ResultsLogEntry logEntry = null;
    try {
      System.out.println("Starting test ");
      System.out.println(loadTestData.toString());
      init();
      System.out.println(uniqueURI);
      for (int i = 0; i < loadTestData.getNumOfLoops() && alive; i++) {

        // results logging
        logEntry = new ResultsLogEntry("Thread " + threadName + " " + uniqueURI);
        logEntry.startTimer();
        connect();
        if (isLoadDir) {
          uriDelta = Integer.toString(i);
          loadTriplesFromDir(loadTestData.getRollback());
        } else if (loadTestData.getGenerateQuery().length() != 0) {
          // loadTriplesFromQuery();
        }
        logEntry.stopTimer();
        ResultsLogger logger = StressManager.getResultsLogger(loadTestData.getLogFileName());
        if (alive)
          verifyIntervalAfterIteration(i+1, logger, logEntry);
        disconnect();

        // update telemetry here
        updateCounter(i);
      }
    } catch (XQueryException e) {
      e.printStackTrace();
      logEntry.stopTimer();
      logEntry.setPassFail(false);
      logEntry.setIdentifier("EXCEPTION: " + e.getCode() + " Thread " + threadName + " " + uniqueURI);
      ResultsLogger logger = 
        StressManager.getResultsLogger(loadTestData.getLogFileName());
      logger.logResult(logEntry);
    } catch (Exception e) {
      e.printStackTrace();
      logEntry.stopTimer();
      logEntry.setPassFail(false);
      logEntry.setIdentifier("EXCEPTION: Thread " + threadName + " " + uniqueURI);
      ResultsLogger logger = 
        StressManager.getResultsLogger(loadTestData.getLogFileName());
      logger.logResult(logEntry);
    }
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

 
  // @Override
  protected void verifyIntervalAfterIteration(int loop,
                    ResultsLogger logger,
                    ResultsLogEntry logEntry)
      throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(queryTestData.concurrency);
    for (int i=0; i<queryTestData.repeat; i++) {
      for (int j=0; j<queryTestData.queries.size(); j++) {
        String query = queryTestData.queries.get(j).query.replaceAll("_LOOP_", Integer.toString(loop));
        ResultsLogEntry thisEntry = logEntry.clone();
        Runnable task = new VerificationTask(query, j, uniqueURI, logger, thisEntry);
        exec.execute(task);
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
    private String query;
    private int queryID;
    private String uniqueURI;
    private ResultsLogger logger;
    private ResultsLogEntry logEntry;
    
    public VerificationTask(String query, int queryID, String uniqueURI,
                ResultsLogger logger, ResultsLogEntry logEntry) {
      this.query = query;
      this.queryID = queryID;
      this.uniqueURI = uniqueURI;
      this.logger = logger;
      this.logEntry = logEntry;
    }    

    @Override
    public void run() {
      long startTime = System.currentTimeMillis();
      long endTime = startTime;

      // try this
      logEntry.startTimer();

      //no db replication support yet
      try {
        int retryMax = VERIFY_RETRY_MAX;
        if (queryTestData.loadTestData.isInsertTime())
          retryMax = 120;
        for (SessionHolder s : SPARQLUpdateTester.this.sessions) {
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

          logEntry.stopTimer();
          logEntry.setInfo("query ID " + queryID);

          if (value.equalsIgnoreCase("true"))
            logEntry.setPassFail(true);
          else
            logEntry.setPassFail(false);

          if (logger != null)
            logger.logResult(logEntry);

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
      } catch (XQueryException x) {
        // TODO:  what to do with these errors?
        logEntry.stopTimer();
        logEntry.setPassFail(false);

        // research to see how long it took for our verify query to CANCEL
        endTime = System.currentTimeMillis();

        if (x.getCode().equals("SVC-CANCELED")) {
          System.out.println("XQueryException Query returned canceled");
          logEntry.setInfo("VERIFY " + x.getCode() +
                            " query ID " + queryID );
          if (logger != null)
            logger.logResult(logEntry);
          // TODO:  trying not terminating if we only got canceled??????
          // alive = false;
        } else if (x.getCode().equals("XDMP-DEADLOCK")) {
          // TODO:  what else comes out in here?
          // I think we expect deadlock, but haven't logged it yet?
          System.out.println("XQueryException Query returned error " + x.getCode());
          logEntry.setInfo("VERIFY " + x.getCode() +
                            " query ID " + queryID );
          if (logger != null)
            logger.logResult(logEntry);
          // deadlock is occasionally expected - don't terminated
          // alive = false;
        } else {
          // TODO:  what else comes out in here?
          // I think we expect deadlock, but haven't logged it yet?
          System.out.println("XQueryException Query returned error " + x.getCode());
          logEntry.setInfo("VERIFY " + x.getCode() +
                            " query ID " + queryID );
          if (logger != null)
            logger.logResult(logEntry);
          alive = false;
        }
      } catch (QueryException x) {
        // TODO:  what to do with these errors?
        logEntry.stopTimer();
        logEntry.setPassFail(false);

        // research to see how long it took for our verify query to CANCEL
        endTime = System.currentTimeMillis();

        if (x.getCode().equals("XDMP-DEADLOCK")) {
          // TODO:  what else comes out in here?
          // I think we expect deadlock, but haven't logged it yet?
          System.out.println("QueryException Query returned error " + x.getCode());
          logEntry.setInfo("VERIFY " + x.getCode() +
                            " query ID " + queryID);
          if (logger != null)
            logger.logResult(logEntry);
          // deadlock is occasionally expected - don't terminated
          // alive = false;
        } else {
          // TODO:  what else comes out in here?
          // I think we expect deadlock, but haven't logged it yet?
          System.out.println("QueryException Query returned error " + x.getCode());
          logEntry.setInfo("VERIFY " + x.getCode() +
                            " query ID " + queryID );
          if (logger != null)
            logger.logResult(logEntry);
          alive = false;
        }
      } catch (Throwable t) {
        t.printStackTrace();
        String error = "EXCEPTION running " + query;
        System.err.println(error);
        System.err.println("Exiting...");

        logEntry.setPassFail(false);
        logEntry.setInfo("EXCEPTION " + t.toString() + " queryID " + queryID );
        if (logger != null)
          logger.logResult(logEntry);

        alive = false;
      }
    }    
  }
}
