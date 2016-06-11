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

import java.io.File;
import java.io.FileInputStream;
import java.util.Date;
import java.util.ArrayList;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.TimeZone;
import java.util.Random;
import java.util.StringTokenizer;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.exceptions.RetryableXQueryException;
import com.marklogic.xcc.exceptions.XQueryException;
import com.marklogic.xcc.exceptions.ServerConnectionException;

import test.utilities.BaseTemplateParser;
import test.telemetry.TelemetryServer;

public class ParserTester extends XccLoadTester {
  // include data class here
  protected ParserTestData loadTestData = null;
  protected boolean isLoadDir = false;
  protected int numLoaded = 0;
  protected String uniqueURI = null;
  protected String lastURI = null;
  protected String verifyCollection = null;
  protected String uriDelta = "";
  protected String randStr = null;
  protected String dateStr = null;
  protected boolean isJSLoad = false; 
  protected Random randNum = null;
  private ArrayList<RetryRecord> dupeList = null;
  private ArrayList<RetryRecord> versionList = null;

  private int testingVal = 0;
  private int currDoc = 0;
  private boolean isFakeVerify = true;

  /**
   * inner class just to put together strings we need to send the same
   * trade through either as a duplicate or with a new version number
   */
  protected class
    RetryRecord {
    String tradeXML;
    String uri;
    String uriDelta;
    String templateName;

    RetryRecord(String tradeXML, String uri, String uriDelta, String templateName) {
      this.tradeXML = tradeXML;
      this.uri = uri;
      this.uriDelta = uriDelta;
      this.templateName = templateName;
    }

  }

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

  public ParserTester(ConnectionData connData, ParserTestData loadData,
                       String threadName) {
    super(connData, loadData, threadName);
    setTestName("ParserTester");
    loadTestData = loadData;
    randNum = new Random();
    dupeList = new ArrayList<RetryRecord>();
    versionList = new ArrayList<RetryRecord>();

    setUniqueURI();
  }

  public void init() throws Exception {
    setLoadDir();
    setUniqueURI();
    setTelemetryData();
    setPrivateTelemetryData();
  }

  protected void setUniqueURI() {
    randStr = randomString(16);
    dateStr = getDateString();
    if (uniqueURI == null)
      uniqueURI = "/" + randStr + "/" + dateStr + "/" + threadName + "/";

    dateStr += threadName;
  }

  // verification queries should always return a number
  protected void verifyLoaded(String verificationQuery, int numLoaded,
      String curURI) throws Exception {
    if (loadTestData.isInsertTime()) 
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

    if (isFakeVerify)
      return;

    if (loadTestData.isInsertTime()) 
      return;
    int serverIndex = 0;
    for (SessionHolder s : sessions) {
      int numForCollection = 1;
      String xqStr = "fn:count(fn:collection('" + verifyCollection + "'))";
      String value = getValueFromResults(s.runQuery(xqStr, null));
      int numInDB = Integer.parseInt(value);
      if (numInDB != numForCollection) {
        String error = "ERROR Loaded " + numLoaded + " collection " + verifyCollection
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
      if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG"))
        s.runQuery(xqStr);

      serverIndex++;
    }
  }
  

  protected void verifyCounts() throws Exception {
    if (loadTestData.isInsertTime()) 
      return;

      ResultsLogEntry logEntry = new ResultsLogEntry("thread " + threadName + 
                                          " verifyCounts");
      logEntry.startTimer();

    int serverIndex = 0;
    for (SessionHolder s : sessions) {
      int numForCollection = 1;
      String xqStr = "xquery version \"1.0-ml\"; ";
      xqStr = xqStr + "(xdmp:estimate(fn:doc()), \" \", ";
      xqStr = xqStr + "xdmp:estimate(xdmp:document-properties()) ) ";
      String value = getValueFromResults(s.runQuery(xqStr, null));
      logEntry.stopTimer();
      logEntry.setInfo("value is " + value);
        logEntry.setPassFail(true);

      StringTokenizer tokens = new StringTokenizer(value);
      int numDocs = Integer.parseInt(tokens.nextToken());
      int numProperties = Integer.parseInt(tokens.nextToken());
      if (numDocs != numProperties) {
        String error = "ERROR Docs don't match properties: docs "
                  + numDocs + ", properties " + numProperties;
        s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        logEntry.setPassFail(false);


        // System.err.println("Exiting...");
        // alive = false;
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

        ResultsLogger logger =
          StressManager.getResultsLogger(loadTestData.getLogFileName());
        if (logger != null)
          logger.logResult(logEntry);

      updateVerifyCounter(numDocs, numProperties);

      if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG"))
        s.runQuery(xqStr);

      serverIndex++;
    }
  }
  
  private String telemetryVerifyCounterDocString = null;
  private String telemetryVerifyCounterPropString = null;
  private String telemetryVerifyCounterDateString = null;
  private SimpleDateFormat verifyFormatter = null;
  private Date verifyDate = null;

  protected void setPrivateTelemetryData() {
    telemetryVerifyCounterDocString = "dbTradeStore.verifyCount.counter.documents";
    telemetryVerifyCounterPropString = "dbTradeStore.verifyCount.counter.properties";
    telemetryVerifyCounterDateString = "dbTradeStore.verifyCount.timestamp";
    verifyDate = new Date();

    verifyFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    TelemetryServer telemetry = StressManager.getTelemetryServer();
    if (telemetry != null) {
      // telemetry.sendTelemetry(telemetryVerifyCounterDocString, "0");
      // telemetry.sendTelemetry(telemetryVerifyCounterPropString, "0");
      telemetry.sendTelemetry(telemetryVerifyCounterDateString,
                verifyFormatter.format(new Date()));
    }
  }

  protected void updateVerifyCounter(int docCount, int propCount) {
    TelemetryServer telemetry = StressManager.getTelemetryServer();
    if (telemetry != null) {
      telemetry.sendTelemetry(telemetryVerifyCounterDocString,
            Integer.toString(docCount));
      telemetry.sendTelemetry(telemetryVerifyCounterPropString,
            Integer.toString(propCount));
      verifyDate.setTime(System.currentTimeMillis());
      telemetry.sendTelemetry(telemetryVerifyCounterDateString,
            verifyFormatter.format(verifyDate));
    }
  }


  protected void verifyIntervalAfterIteration(int loop) throws Exception {

    if ((loop % 100) == 0)
      verifyCounts();
  }

  protected void setLoadDir() {
    if (!(loadTestData.getLoadDir() == null || loadTestData.getLoadDir().equals("")))
      isLoadDir = true;
  }

  protected File[] listFiles(String path) {
    File dir = new File(path);
    ArrayList<File> files = new ArrayList<File>();
    for (File f : dir.listFiles())
      if (!f.isDirectory())
        files.add(f);
    return files.toArray(new File[0]);
  };

  protected void processDuplicates()
      throws InterruptedException {
    File[] fileList = listFiles(loadTestData.getLoadDir());
    int curBatch = 0;
    int batchStart = 0;
    int retryCount = 0;
    String verificationQuery = "";

    // System.out.println("processDuplicates()");

    int i = 0;
    while (i < dupeList.size() && alive) {
      boolean inserted = false;
      try {
        if (curBatch == 0) {
          beginTransaction();
          batchStart = i;
        }

        RetryRecord dupe = (RetryRecord)dupeList.get(i);

        ContentCreateOptions options = null;
        options = ContentCreateOptions.newXmlInstance();
        options.setFormatXml();
        String[] collections = { uniqueURI };
        options.setCollections(collections);

        String curURI = dupe.uri;
        String tradeXml = dupe.tradeXML;
        String delta = dupe.uriDelta;
        String templateName = dupe.templateName;

        // System.out.println("processing " + curURI);

        if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG")) {
          // DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          // Date date = new Date();
          // System.out.println("Loading " + curURI + " at " + dateFormat.format(date));
        }

        ResultsLogEntry logEntry = null;
        String returnedURI = null;

        for (SessionHolder s : sessions) {
          long startTime = System.currentTimeMillis();
        
          logEntry = new ResultsLogEntry("thread " + threadName + 
                                          " template " + templateName +
                                          " duplicate_" + curURI);
          logEntry.startTimer();

          // THIS IS WHERE WE SEND THE DOCUMENT OVER
          //
          //  s.session.insertContent(content);
          //  s.runQuery(myQueryGoesHere);
          //
          Request request = s.session.newModuleInvoke("/stress/catalyst-main-module.xqy");
          // declare variable $mydoc as xs:string external
          // $mydoc as xs:string now exists
          request.setNewVariable("mydoc", ValueType.XS_STRING, tradeXml);
          request.setNewVariable("myuri", ValueType.XS_STRING, curURI);
          request.setNewVariable("myindex", ValueType.XS_INTEGER, Integer.parseInt(delta));
          request.setNewVariable("myrawmessage", ValueType.XS_STRING, "DbTradeStoreLoadTester dupe");
          ResultSequence rs = s.session.submitRequest(request);
          returnedURI = rs.asString();
          lastURI = returnedURI;

          // we passed in a URI, and it ends up being the collection, preceded by load_
          // we an look that up for validation
          verifyCollection = "duplicate_" + curURI;

          logEntry.stopTimer();
          logEntry.setPassFail(true);
          ResultsLogger logger =
            StressManager.getResultsLogger(loadTestData.getLogFileName());
          logger.logResult(logEntry);

          long elapsed = System.currentTimeMillis() - startTime;
          if (elapsed > 60 * 1000) {
           System.out.println("Took too long to load (" + elapsed + "): " + curURI);
          }
          totalTime += elapsed;
        }
        inserted = true;

        // if the current batch size is equal to the desired batch size,
        // or if this is the last file in the directory commit or rollback
            commitTransaction();
            numLoaded += curBatch;
            // verify all documents from this commit are loaded
            for (int pos = i; curBatch > 0; --pos) {
              --curBatch;
            /*
              curURI = uniqueURI + uriDelta + fileList[pos].getName();
            */
              verificationQuery = "fn:count(fn:doc('" + returnedURI + "'))";
              verifyLoaded(verificationQuery, 1, curURI);
            }

        // throttles the work contributed by this thread
        sleepBetweenIterations();

        // move forward
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

  protected void processVersionIncrements()
      throws InterruptedException {
    File[] fileList = listFiles(loadTestData.getLoadDir());
    int curBatch = 0;
    int batchStart = 0;
    int retryCount = 0;
    String verificationQuery = "";

    // System.out.println("processVersionIncrements()");

    int i = 0;
    while (i < versionList.size() && alive) {
      boolean inserted = false;
      try {
        if (curBatch == 0) {
          beginTransaction();
          batchStart = i;
        }

        RetryRecord vers = (RetryRecord)versionList.get(i);

        ContentCreateOptions options = null;
        options = ContentCreateOptions.newXmlInstance();
        options.setFormatXml();
        String[] collections = { uniqueURI };
        options.setCollections(collections);

        String curURI = vers.uri;
        String tradeXml = vers.tradeXML;
        String delta = vers.uriDelta;
        String templateName = vers.templateName;

        // System.out.println("processing " + vers.uri);

        if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG")) {
          // DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          // Date date = new Date();
          // System.out.println("Loading " + curURI + " at " + dateFormat.format(date));
        }

        ResultsLogEntry logEntry = null;
        String returnedURI = null;

        for (SessionHolder s : sessions) {
          long startTime = System.currentTimeMillis();
        
          logEntry = new ResultsLogEntry("thread " + threadName + 
                                          " template " + templateName +
                                          " increment " +
                                          " load_" + curURI);
          logEntry.startTimer();

          // THIS IS WHERE WE SEND THE DOCUMENT OVER
          //
          //  s.session.insertContent(content);
          //  s.runQuery(myQueryGoesHere);
          //
          Request request = s.session.newModuleInvoke("/stress/db-version-incr-module.xqy");
          // declare variable $mydoc as xs:string external
          // $mydoc as xs:string now exists
          request.setNewVariable("mydoc", ValueType.XS_STRING, tradeXml);
          request.setNewVariable("myuri", ValueType.XS_STRING, curURI);
          request.setNewVariable("myindex", ValueType.XS_INTEGER, Integer.parseInt(delta));
          request.setNewVariable("myrawmessage", ValueType.XS_STRING, "DbTradeStoreLoadTester increment");
          ResultSequence rs = s.session.submitRequest(request);
          returnedURI = rs.asString();
          lastURI = returnedURI;

          // we passed in a URI, and it ends up being the collection, preceded by load_
          // we an look that up for validation
          verifyCollection = "load_" + curURI;

          logEntry.stopTimer();
          logEntry.setPassFail(true);
          ResultsLogger logger =
            StressManager.getResultsLogger(loadTestData.getLogFileName());
          logger.logResult(logEntry);

          long elapsed = System.currentTimeMillis() - startTime;
          if (elapsed > 60 * 1000) {
           System.out.println("Took too long to load (" + elapsed + "): " + curURI);
          }
          totalTime += elapsed;
        }
        inserted = true;

        // if the current batch size is equal to the desired batch size,
        // or if this is the last file in the directory commit or rollback
            commitTransaction();
            numLoaded += curBatch;
            // verify all documents from this commit are loaded
            for (int pos = i; curBatch > 0; --pos) {
              --curBatch;
            /*
              curURI = uniqueURI + uriDelta + fileList[pos].getName();
            */
              verificationQuery = "fn:count(fn:doc('" + returnedURI + "'))";
              verifyLoaded(verificationQuery, 1, curURI);
            }

        // throttles the work contributed by this thread
        sleepBetweenIterations();

        // move forward
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

  protected void loadContentFromDir(boolean rollback)
      throws InterruptedException {
    File[] fileList = null;
    int curBatch = 0;
    int batchStart = 0;
    int retryCount = 0;
    String verificationQuery = "";

    if (loadTestData.getLoadDir() != null)
      fileList = listFiles(loadTestData.getLoadDir());
    else if (loadTestData.getLoadTemplate() != null) {
      fileList = new File[1];
      fileList[0] = new File(loadTestData.getLoadTemplate());
      // System.out.println("loading test template " + loadTestData.getLoadTemplate());
    } else {
      System.out.println("No template dir or file specified");
      return;
    }

    int i = 0;
    while (i < fileList.length && alive) {
      boolean inserted = false;
      try {
        if (curBatch == 0) {
          beginTransaction();
          batchStart = i;
        }

        // handle the random content
        String lastName = null;
        String firstName = null;
        int rvar = -1;
        while (rvar < 0) {
          rvar = randNum.nextInt() % loadTestData.lastnames.size();
        }
        lastName = loadTestData.lastnames.get(rvar);

        rvar = -1;
        while (rvar < 0) {
          rvar = randNum.nextInt() % loadTestData.firstnames.size();
        }
        firstName = loadTestData.firstnames.get(rvar);

        ContentCreateOptions options = null;
        options = ContentCreateOptions.newXmlInstance();
        options.setFormatXml();
        String[] collections = { uniqueURI };
        options.setCollections(collections);
        // String curURI = uniqueURI + uriDelta + fileList[i].getName();


        // TODO: this is where we get the testing suffix

        String curURI = "/case/testing" + testingVal + "/document/" + currDoc;
        String templateName = fileList[i].getName();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        String fullpath = null;
        if (loadTestData.getLoadDir() == null) {
          fullpath = loadTestData.getLoadTemplate();
        } else {
          fullpath = loadTestData.getLoadDir() + File.separator +
                          fileList[i].getName();
        }
        FileInputStream fis = new FileInputStream(fullpath);

        BaseTemplateParser parser =
          new BaseTemplateParser();

        parser.initialize();
        parser.setFieldManager(loadTestData.fieldManager);
        parser.parseTemplate(fis, baos);
        // clean this up explicitly to facilitate GC
        fis.close();
        fis = null;
        String tradeXml = baos.toString("UTF-8");
        // seeing if this will clean up the dangling zip streams
        parser.cleanup();

        tradeXml = tradeXml.replaceAll("__LASTNAME__", lastName);
        tradeXml = tradeXml.replaceAll("__FIRSTNAME__", firstName);
        // System.out.println("lastname, firstname is " + lastName + ", " + firstName);

        if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG")) {
          DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          Date date = new Date();
          // System.out.println("Loading " + curURI + " at " + dateFormat.format(date));
        }

        ResultsLogEntry logEntry = null;
        String returnedURI = null;

        for (SessionHolder s : sessions) {
          long startTime = System.currentTimeMillis();
        
          logEntry = new ResultsLogEntry("thread " + threadName + 
                                          " template " + templateName +
                                          " load_" + curURI);
          logEntry.startTimer();

          // THIS IS WHERE WE SEND THE DOCUMENT OVER
          //
          //  s.session.insertContent(content);
          //  s.runQuery(myQueryGoesHere);
          //
          Request request = s.session.newModuleInvoke("/stress/catalyst-main-module.xqy");
          // declare variable $mydoc as xs:string external
          // $mydoc as xs:string now exists
          request.setNewVariable("mydoc", ValueType.XS_STRING, tradeXml);
          request.setNewVariable("myuri", ValueType.XS_STRING, curURI);
          request.setNewVariable("myindex", ValueType.XS_INTEGER, Integer.parseInt(uriDelta));
          request.setNewVariable("myrawmessage", ValueType.XS_STRING, "DbTradeStoreLoadTester");
          ResultSequence rs = s.session.submitRequest(request);
          returnedURI = rs.asString();
          lastURI = returnedURI;

          // we passed in a URI, and it ends up being the collection, preceded by load_
          // we an look that up for validation
          verifyCollection = "load_" + curURI;

          logEntry.stopTimer();
          logEntry.setPassFail(true);
          ResultsLogger logger =
            StressManager.getResultsLogger(loadTestData.getLogFileName());
          logger.logResult(logEntry);

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
            /*
              curURI = uniqueURI + uriDelta + fileList[pos].getName();
            */
              verificationQuery = "fn:count(fn:doc('" + returnedURI + "'))";
              verifyLoaded(verificationQuery, 1, curURI);
            }
          } else {
            rollbackTransaction();
            // verify all documents gone b/c rollback
            for (int pos = i; curBatch > 0; --pos) {
              --curBatch;
            /*
              curURI = uniqueURI + uriDelta + fileList[pos].getName();
              verificationQuery = "fn:count(fn:doc('" + curURI + "'))";
              verifyLoaded(verificationQuery, 0, curURI);
            */
            }
          }
          // current batch has been added to total so 0 it out
          curBatch = 0;
          retryCount = 0;
        }

        // throttles the work contributed by this thread
        sleepBetweenIterations();

        // check documents in db at interval
        // if multistmt have to check at batch end
        if (numLoaded % loadTestData.getCheckInterval() == 0 && curBatch == 0
            && !rollback) {
          verifyInterval();
        }

        if (numLoaded % loadTestData.getDuplicateInterval() == 0 && !rollback) {
          
          RetryRecord dupe = new RetryRecord(tradeXml, curURI, uriDelta, templateName);

          dupeList.add(dupe);
        }

        if (numLoaded % loadTestData.getVersionIncrementInterval() == 0 && !rollback) {
          
          RetryRecord incr = new RetryRecord(tradeXml, curURI, uriDelta, templateName);

          versionList.add(incr);
        }

        // move forward
        ++i;
        ++currDoc;
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
      System.out.println(loadTestData.toString());
      init();
      System.out.println(uniqueURI);

      // we're going to loop through testing params 1-9 while looping through
      // doc 0-65536

      int maxDoc = 65535;  // 65535
      currDoc = 0;

      // if we have starting/ending ranges, use them
      int testEndId = loadTestData.getEndId();
      if (testEndId != -1) {
        currDoc = loadTestData.getStartId();
        maxDoc = loadTestData.getEndId();
      }

      testingVal = loadTestData.getTestingSuffix();

/*
      for (testingVal = 1; testingVal <= 9; testingVal++) {
*/
        while (currDoc < maxDoc) {
          updateCounter(currDoc);
          connect();
          if (isLoadDir) {
            uriDelta = Integer.toString(currDoc);
            loadContentFromDir(loadTestData.getRollback());
          } else if (loadTestData.getLoadTemplate() != null) {
            // a single template - let's do this!
            uriDelta = Integer.toString(currDoc);
            loadContentFromDir(loadTestData.getRollback());
          } else if (loadTestData.getGenerateQuery().length() != 0) {
            // loadContentFromQuery();
          }
          /*
          if (alive)
            verifyIntervalAfterIteration(i+1);
          */
          disconnect();

          if (!alive)
            break;
        }
/*
      }
*/

      // now that we're through, try the duplicates
      connect();
      // processDuplicates();
      disconnect();

      // now try the version increments
      connect();
      // processVersionIncrements();
      disconnect();

      Thread.sleep(120000L);

    } catch (Exception e) {
      e.printStackTrace();
    }

    // report the results
    System.out.println("Thread " + threadName + " complete:  total loaded " + numLoaded);

    alive = false;

  }
}
