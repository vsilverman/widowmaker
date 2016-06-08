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

import java.io.File;
import java.util.Date;
import java.util.ArrayList;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.TimeZone;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.exceptions.RetryableXQueryException;
import com.marklogic.xcc.exceptions.XQueryException;
import com.marklogic.xcc.exceptions.ServerConnectionException;

import test.telemetry.TelemetryServer;


public class XccInvokeTester extends XccStressTester {
  // include data class here
  protected InvokeTestData invokeTestData = null;
  protected boolean isLoadDir = false;
  protected int numLoaded = 0;
  protected String uniqueURI = null;
  protected String uriDelta = "";
  protected String randStr = null;
  protected String dateStr = null;
  protected boolean isJSLoad = false; 

  protected boolean doVerification = false;

  protected class
    ReplicaValNotifier
      extends ReplicaValidationNotifierImpl
    {
      private ValidationData vData;

      ReplicaValNotifier()
      {
        super();
      }
      ReplicaValNotifier(ValidationData data)
      {
        super();
        vData = data;
      }

      void  setValidationData(ValidationData data)
      {
        vData = data;
      }
    }

  public XccInvokeTester(ConnectionData connData, InvokeTestData invokeData,
                       String threadName) {
    super(connData, invokeData, threadName);
    invokeTestData = invokeData;
    setUniqueURI();
  }

  public void init() throws Exception {
    setLoadDir();
    setUniqueURI();
    setTelemetryData();
  }

  protected void setUniqueURI() {
    randStr = randomString(16);
    dateStr = getDateString();
    if (uniqueURI == null)
      uniqueURI = "/" + randStr + "/" + dateStr + "/" + threadName + "/";

    dateStr += threadName;
  }

  private String telemetryCounterString = null;
  private String telemetryTestName = null;
  private String telemetryTestString = null;
  private String telemetryStartString = null;

  /**
   * should be called only by init
   */
  protected void setTelemetryData() {
    telemetryCounterString = "testthread." + threadName + ".counter";
    telemetryTestString = "testthread." + threadName + ".testname";
    telemetryStartString = "testthread." + threadName + ".starttime";
    if (telemetryTestName == null)
      setTestName("XccInvokeTester");
    TelemetryServer telemetry = StressManager.getTelemetryServer();
    if (telemetry != null) {
      telemetry.sendTelemetry(telemetryCounterString, "0");
      telemetry.sendTelemetry(telemetryTestString, telemetryTestName);
      Date d = new Date();
      telemetry.sendTelemetry(telemetryStartString, d.toString());
    }
  }

  protected void setTestName(String s) {
    telemetryTestName = s;
    TelemetryServer telemetry = StressManager.getTelemetryServer();
    if ((telemetry != null) && (telemetryTestString != null)) {
      telemetry.sendTelemetry(telemetryTestString, telemetryTestName);
    }
  }

  protected void updateCounter(int i) {
    TelemetryServer telemetry = StressManager.getTelemetryServer();
    if (telemetry != null) {
      telemetry.sendTelemetry(telemetryCounterString, Integer.toString(i));
    }
  }

  // verification queries should always return a number
  protected void verifyLoaded(String verificationQuery, int numLoaded,
      String curURI) throws Exception {
    if (invokeTestData.isInsertTime()) 
      return;
    int serverIndex = 0;
    for (SessionHolder s : sessions) {
      String value = getValueFromResults(s.runQuery(verificationQuery, null));
      int numInDB = Integer.parseInt(value);
      if (numLoaded != numInDB) {
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

  protected void verifyInterval() throws Exception {

    // not sure what verification is for this test - let's shortcircuit for now
    if (!doVerification)
      return;

    if (invokeTestData.isInsertTime()) 
      return;
    int serverIndex = 0;
    for (SessionHolder s : sessions) {
      String xqStr = "fn:count(fn:collection('" + uniqueURI + "'))";
      String value = getValueFromResults(s.runQuery(xqStr, null));
      int numInDB = Integer.parseInt(value);
      if (numInDB != numLoaded) {
        String error = "ERROR Loaded " + numLoaded + " collection " + uniqueURI
            + " contained " + numInDB;
        s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
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
      if (invokeTestData.getLogOption().equalsIgnoreCase("DEBUG"))
        s.runQuery(xqStr);

      serverIndex++;
    }
  }
  
  protected void verifyIntervalAfterIteration(int loop) throws Exception {
    verifyInterval();
  }

  protected void setLoadDir() {
    if (!invokeTestData.getLoadDir().equals(""))
      isLoadDir = true;
  }

  protected void invokeModule(boolean rollback)
      throws InterruptedException {
    int curBatch = 0;
    int batchStart = 0;
    int retryCount = 0;
    String verificationQuery = "";
    String returnedResult = "";

    int i = 0;
    // not sure what we're looping on
    int something = 1;
    ResultsLogEntry logEntry = null;

    while (i < something && alive) {
      boolean inserted = false;
      try {
        if (curBatch == 0) {
          // beginTransaction();
          batchStart = i;
        }

        String moduleName = invokeTestData.getModuleName();

        for (SessionHolder s : sessions) {
          long startTime = System.currentTimeMillis();
          String hostName = s.session.getConnectionUri().getHost();
          logEntry = new ResultsLogEntry("thread " + threadName + 
                                          " host " + hostName +
                                          " module " + moduleName);
          logEntry.startTimer();

          // THIS IS WHERE WE SEND THE DOCUMENT OVER
          //
          // System.out.println("invoking module " + moduleName);
          // System.out.println("contacting host:  " + hostName);
          Request request = s.session.newModuleInvoke(moduleName);
          // declare variable $mydoc as xs:string external
          // $mydoc as xs:string now exists
          request.setNewVariable("format", ValueType.XS_STRING,
                          invokeTestData.getResponseFormat());
          request.setNewVariable("myrawmessage", ValueType.XS_STRING, "XccInvokeTester increment");

          ResultSequence rs = s.session.submitRequest(request);
          logEntry.stopTimer();
          String[] results = rs.asStrings();
          returnedResult = results[0];
          // System.out.println("result from invoke:  " + returnedResult);
          logEntry.setPassFail(true);
          ResultsLogger logger =
                StressManager.getResultsLogger(invokeTestData.getLogFileName());
          logger.logResult(logEntry);

          long elapsed = System.currentTimeMillis() - startTime;
          if (elapsed > 60 * 1000) {
           System.out.println("Took too long to load (" + elapsed + "): ");
          }
          totalTime += elapsed;
        }
        inserted = true;

        // throttles the work contributed by this thread
        Thread.sleep(invokeTestData.getMaxSleepTime());

        // check documents in db at interval
        // if multistmt have to check at batch end
        if (numLoaded % invokeTestData.getCheckInterval() == 0 && curBatch == 0
            && !rollback)
          verifyInterval();

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
        // TODO:  is this a hash of error codes that indicate we have a bad document?
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
        } else if (e.getCode().equals("XDMP-MULTIROOT") && retryCount < 100) {
          System.out.println("JJAMES: Insert failed for XDMP-MULTIROOT: " +
                uniqueURI + ", " + retryCount);
          if (inserted) {
            i++;
            curBatch = 0;
          }
    else
    {
      // need to figure out how not to retry
      i++;
    }
          Thread.sleep(5000);
          continue;
        } else if (e.getCode().equals("XDMP-DOCSTARTTAGCHAR") && retryCount < 100) {
          System.out.println("JJAMES: Insert failed for XDMP-DOCSTARTTAGCHAR: " +
                uniqueURI + ", " + retryCount);
          if (inserted) {
            i++;
            curBatch = 0;
          }
    else
    {
      // need to figure out how not to retry
      i++;
    }
          Thread.sleep(5000);
          continue;
        } else if (e.getCode().equals("XDMP-DOCUNEOF") && retryCount < 100) {
          System.out.println("JJAMES: Insert failed for XDMP-DOCUNEOF: " +
                uniqueURI + ", " + retryCount);
          if (inserted) {
            i++;
            curBatch = 0;
          }
    else
    {
      // need to figure out how not to retry
            i++;
    }
          Thread.sleep(5000);
          continue;
        } else if (e.getCode().equals("XDMP-DOCUTF8SEQ") && retryCount < 100) {
          System.out.println("JJAMES: Insert failed for XDMP-DOCUTF8SEQ: " +
                uniqueURI + ", " + retryCount);
          if (inserted) {
            i++;
            curBatch = 0;
          }
    else
    {
      // need to figure out how not to retry
            i++;
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
//add in the collections
  private void jsLoad(String fileName, String uri, SessionHolder s) throws Exception{
  //run a js query that does a document-get and and document-insert
  String jsQuery = "declareUpdate() \n" + 
    "xdmp.documentInsert('"+ uri + "'" + " ,xdmp.documentGet('" + fileName + "'), xdmp.defaultPermissions(), '"+ uniqueURI + "' ) "; 
  if( testData.getMultiStatement() ) jsQuery = "xdmp.documentInsert('"+ uri + "'" + " ,xdmp.documentGet('" + fileName + "'), xdmp.defaultPermissions(), '"+ uniqueURI + "' ) ";
  System.out.println( jsQuery ); 
  Query query = new Query(jsQuery, "javascript"); 
        s.runQuery(query, null);
  }
  // METHOD runTest() a virtual method, sub classes must implement
  public void runTest() {
    try {
      System.out.println("Starting test ");
      System.out.println(invokeTestData.toString());
      init();
      System.out.println(uniqueURI);
      for (int i = 0; i < invokeTestData.getNumOfLoops() && alive; i++) {
        connect();
        if (isLoadDir) {
          uriDelta = Integer.toString(i);
          invokeModule(invokeTestData.getRollback());
          updateCounter(i);
        } else if (invokeTestData.getGenerateQuery().length() != 0) {
          // loadContentFromQuery();
        }
        if (alive)
          verifyIntervalAfterIteration(i+1);
        disconnect();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    // report the results
    System.out.println("Thread " + threadName + " complete:  total loaded " + numLoaded);

    alive = false;

  }
}
