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

package test.sql;

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.Date;
import java.util.ArrayList;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.TimeZone;
import java.util.HashMap;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Iterator;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XdmItem;
import com.marklogic.xcc.exceptions.RetryableXQueryException;
import com.marklogic.xcc.exceptions.XQueryException;
import com.marklogic.xcc.exceptions.ServerConnectionException;

import org.w3c.dom.Node;

import test.utilities.ParseField;
import test.utilities.BaseParseField;
import test.utilities.BaseParseFieldManager;
import test.utilities.ParseFieldManager;
import test.utilities.SqlTemplateParser;

import test.utilities.MarkLogicWrapper;
import test.utilities.MarkLogicWrapperFactory;
import test.utilities.GraphUtils;

import test.telemetry.TelemetryServer;

import test.stress.StressManager;
import test.stress.XccLoadTester;
import test.stress.ResultsLogger;
import test.stress.ResultsLogEntry;
import test.stress.ValidationData;
import test.stress.Query;
import test.stress.ConnectionData;
import test.stress.ReplicaValidationNotifierImpl;
import test.stress.LoadableStressTester;
import test.stress.TestData;
import test.stress.QueryTestData;



import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;
import org.w3c.dom.Attr;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.SAXException;



public class SqlQueryTester extends XccLoadTester
      implements LoadableStressTester {
  public static boolean SKIP_VERIFICATION_DEFAULT = true;

  // include data class here
  protected SqlQueryTestData queryTestData = null;
  protected boolean isLoadDir = false;
  protected int numQueried = 0;
  protected String uniqueURI = null;
  protected String lastURI = null;
  protected String verifyCollection = null;
  protected String uriDelta = "";
  protected String randStr = null;
  protected String dateStr = null;
  protected boolean isJSLoad = false; 
  protected Random randNum = null;
  protected int currentLoop = 0;

  protected TrackingDataManager tracker = null;

  protected boolean skipVerification =  SKIP_VERIFICATION_DEFAULT;
  protected boolean dumpTradeXml = false;
  protected boolean debugFlag = false;

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

  public String getTestType() {
    return "SqlQueryTester";
  }

  public TestData getTestDataInstance(String filename)
      throws Exception {
    return new SqlQueryTestData(filename);
  }

  public SqlQueryTester(ConnectionData connData, SqlQueryTestData queryData,
                       String threadName) {
    super(connData, queryData.getLoadTestData(), threadName);
    setTestName("SqlQueryTester");
    queryTestData = queryData;
    randNum = new Random();

    setUniqueURI();
  }

  public SqlQueryTester() {
    randNum = new Random();
    // setUniqueURI();
  }

  public void initialize(TestData testData, String threadName) {
    queryTestData = (SqlQueryTestData)testData;
    super.initialize(queryTestData.getLoadTestData(), threadName);
    setUniqueURI();
  }

  public void init() throws Exception {
    setTestName("SqlQueryTester");
    setLoadDir();
    setUniqueURI();
    setTelemetryData();
    setPrivateTelemetryData();
    setParseFieldData();

/*
    queryTestData.fieldManager.dumpFieldList(System.out);
*/

    tracker = TrackingDataManager.getTracker();

  }

  public void cleanup() {
    if (tracker != null)
      tracker.cleanup();
  }

  protected void setUniqueURI() {
    randStr = randomString(16);
    dateStr = getDateString();
    if (uniqueURI == null)
      uniqueURI = "/" + randStr + "/" + dateStr + "/" + threadName + "/";

    dateStr += threadName;
  }

  protected void setParseFieldData() {

  }

  String findElement(Node node, String elementToFind) {
    String rval = null;

    return rval;
  }

  /**
   * utilities to find the string that is the value of a specified element
   * this needs to move to a utility class
   */
  String getElementValue(String xmlFile, String elementToFind) {

    if ((xmlFile == null) || (elementToFind == null))
      return null;

    ByteArrayInputStream bais = new ByteArrayInputStream(xmlFile.getBytes());

    return getElementValue(bais, elementToFind);
  }

  String getElementValue(InputStream xmlFile, String elementToFind) {

    if ((xmlFile == null) || (elementToFind == null))
      return null;

    DocumentBuilderFactory dbFactory;
    DocumentBuilder dBuilder;
    Document doc = null;
    String rval = null;
    Node rootNode = null;

    try {
      dbFactory = DocumentBuilderFactory.newInstance();
      dBuilder = dbFactory.newDocumentBuilder();
      doc = dBuilder.parse(xmlFile);
      doc.getDocumentElement().normalize();
      rootNode = doc.getDocumentElement();
    } catch (ParserConfigurationException e) {
      e.printStackTrace();
    }
    catch (SAXException e) {
      e.printStackTrace();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    finally {
      try {
        xmlFile.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    rval = getElementValue(rootNode, elementToFind);

    return rval;
  }

  String getElementValue(Node node, String elementToFind) {

    if ((node == null) || (elementToFind == null)) {
      return null;
    }

    String val = null;
    NodeList nodeList = node.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      if (nodeList.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      Element el = (Element)nodeList.item(i);
      String tagName = el.getTagName();
      if (tagName.equals(elementToFind)) {
        val = getNodeText(el);
        // debug it here
        return val;
      }
      NodeList list = el.getChildNodes();
      val = getElementValue(el, elementToFind);
      if (val != null) {
        return val;
      }
    }

    return val;
  }

  private boolean isWhitespaceNode(Node t) {
    if (t.getNodeType() == org.w3c.dom.Node.TEXT_NODE) {
      String val = t.getNodeValue();
      return val.trim().length() == 0;
    }
    else
      return false;
  }

  protected String getNodeText(Node t) {
    if ((t == null) || (t.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE))
      return null;
    NodeList children = t.getChildNodes();
    String text = "";
    for (int c = 0; c < children.getLength(); c++) {
      Node child = children.item(c);
      if ((child.getNodeType() == org.w3c.dom.Node.TEXT_NODE)
          || (child.getNodeType() == org.w3c.dom.Node.CDATA_SECTION_NODE))
      {
        if (!isWhitespaceNode(child))
          text += child.getNodeValue();
      }
    }
    return text;
  }

  // verification queries should always return a number
  protected void verifyLoaded(String verificationQuery, int numLoaded,
      String curURI) throws Exception {
    if (queryTestData.isInsertTime()) 
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
    if (queryTestData.isInsertTime()) 
      return;

    if (skipVerification)
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
      if (queryTestData.getLogOption().equalsIgnoreCase("DEBUG"))
        s.runQuery(xqStr);

      serverIndex++;
    }
  }
  

  protected void verifyCounts() throws Exception {
    if (queryTestData.isInsertTime()) 
      return;

    if (skipVerification)
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
          StressManager.getResultsLogger(queryTestData.getLogFileName());
        if (logger != null)
          logger.logResult(logEntry);

      updateVerifyCounter(numDocs, numProperties);

      if (queryTestData.getLogOption().equalsIgnoreCase("DEBUG"))
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
    telemetryVerifyCounterDocString = "TemplateData.verifyCount.counter.documents";
    telemetryVerifyCounterPropString = "TemplateData.verifyCount.counter.properties";
    telemetryVerifyCounterDateString = "TemplateData.verifyCount.timestamp";
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

/*
    if ((loop % 100) == 0)
      verifyCounts();
*/
  }

  // TODO:  is this necessary for pure query?
  protected void setLoadDir() {
/*
    if (!queryTestData.getLoadDir().equals(""))
      isLoadDir = true;
*/
  }

  protected void runQueries()
      throws InterruptedException {


    int curBatch = 0;
    int batchStart = 0;
    int retryCount = 0;
    String verificationQuery = "";
    String curURI = null;
    String queryResults = null;

    ResultsLogEntry logEntry = null;
    String tradeXml = null;

    // TODO:  this needs to be re-examined
    // the retry logic seems to operate on incrementing
    // or not incrementing i, but the for loop moves forward
    // anyway when error handling calls continue
    int i = 0;
    for ( Query q : queryTestData.getQueries() ) {

      try {

        ContentCreateOptions options = null;

        String query = q.query;
        int queryId = i;
        ByteArrayInputStream bais = new ByteArrayInputStream(query.getBytes());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        SqlTemplateParser parser =
          new SqlTemplateParser();

        parser.setFieldManager(queryTestData.fieldManager);
        parser.initialize();
        parser.parseTemplate(bais, baos);

        // clean this up explicitly to facilitate GC
        bais.close();
        bais = null;
        tradeXml = baos.toString("UTF-8");
        // seeing if this will clean up the dangling zip streams
        parser.cleanup();

/*
        if (queryTestData.getLogOption().equalsIgnoreCase("DEBUG")) {
          DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          Date date = new Date();
          System.out.println("Loading " + curURI + " at " + dateFormat.format(date));
        }
*/

        if (dumpTradeXml) {
          System.out.println("tradeXml");
          System.out.println(tradeXml);
        }

        // what the heck - wrap the query here


        for (SessionHolder s : sessions) {
          long startTime = System.currentTimeMillis();
        
          logEntry = new ResultsLogEntry("thread " + threadName + 
                                          " query " + queryId );
          logEntry.startTimer();

          boolean displayResults = false;
          boolean displayQuery = false;
          boolean displayHeader = false;
          String val = q.getAttribute("debug");
          if (val != null) {
            // System.out.println("debug is set on this query:  " + val);
            displayResults = Boolean.parseBoolean(val);
          }
          val = q.getAttribute("show_query");
          if (val != null) {
            // System.out.println("show_query is set on this query:  " + val);
            displayQuery = Boolean.parseBoolean(val);
          }
          val = q.getAttribute("show_header");
          if (val != null) {
            // System.out.println("show_header is set on this query:  " + val);
            displayHeader = Boolean.parseBoolean(val);
          }

          if (displayQuery) {
            System.out.println(tradeXml);
          }

          Request request = s.session.newModuleInvoke("/stress/sql-query-runner.xqy");
          request.setNewVariable("sql_query", ValueType.XS_STRING, tradeXml);
          ResultSequence rs = s.session.submitRequest(request);

          int resultCount = 0;

          String serverResult = null;
          String errorLine = null;

          Iterator results = rs.iterator();
          while (results.hasNext()) {
            ResultItem rsItem = (ResultItem)results.next();
            XdmItem item = rsItem.getItem();
            queryResults = item.asString();
            
            // change of plans:  we're going to return SUCCESS or ERROR before the results
            if (resultCount == 0) {
              serverResult = queryResults;
              // System.out.println("result on server:  " + serverResult);
            } else {
              // if we had a SQL error on server, this line has the failure desc
              if (resultCount == 1) {
                errorLine = queryResults;
              }
              // always make sure we print the header, just to see how we did
              if ((displayResults) || (displayHeader && (resultCount == 1)))
                System.out.println(queryResults);
            }
            ++resultCount;
          }
          if (debugFlag)
            System.out.println("total results:  " + resultCount);

          // System.out.println("returnedURI, primaryUri = " + returnedURI + ", " + primaryUri);

          ++numQueried;

          logEntry.stopTimer();
          if (serverResult.equals("SUCCESS")) {
            logEntry.setPassFail(true);
            logEntry.setInfo("total results: " + resultCount);
          } else {
            logEntry.setPassFail(false);
            logEntry.setInfo("SQL error: " + errorLine);
          }
          ResultsLogger logger =
            StressManager.getResultsLogger(queryTestData.getLogFileName());
          logger.logResult(logEntry);

          long elapsed = System.currentTimeMillis() - startTime;
          if (elapsed > 60 * 1000) {
            System.out.println("Took too long to load (" + elapsed + "): Query " + i);
            System.out.println(tradeXml);
          }
          totalTime += elapsed;
        }

        // throttles the work contributed by this thread
        sleepBetweenIterations();

        // move forward
        ++i;
      } catch (RetryableXQueryException e) {
        try {
          // rollbackTransaction();
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
        String error = "ERROR could not load URI : " + curURI;
        System.err.println(error);
        System.exit(1);
      } catch (XQueryException e) {
        // retry for XDMP-FORESTNID
        ++retryCount;
        // TODO:  is this a hash of error codes that indicate we have a bad document?
        // TODO:  THIS GOT ADDED - NEED TO MONITOR

        logEntry.stopTimer();
        logEntry.setPassFail(false);
        logEntry.setInfo(e.getCode());
        ResultsLogger logger =
            StressManager.getResultsLogger(queryTestData.getLogFileName());
        logger.logResult(logEntry);

        if ((e.getCode().equals("XDMP-MEMCANCELED") || e.getCode().equals("SVC-MEMCANCELED"))
               && retryCount < 100) {
          System.out.println("Retry for " + e.getCode() + ": " + retryCount);
          System.out.println("MEMCANCELED query:");
          System.out.println(tradeXml);
          if (retryCount >= 100) {
            System.out.println("Retry for XDMP-MEMCANCELED is exhausted");
            ++i;
            retryCount = 0;
          }
          continue;
        } else if ((e.getCode().equals("XDMP-EXTIME") || e.getCode().equals("SVC-EXTIME"))
               && retryCount < 100) {
          System.out.println("Retry for " + e.getCode() + ": " + retryCount);
          System.out.println("EXTIME query:");
          System.out.println(tradeXml);
          if (retryCount >= 100) {
            System.out.println("Retry for XDMP-EXTIME is exhausted");
            ++i;
            retryCount = 0;
          }
          continue;
        } else if (e.getCode().equals("XDMP-FORESTNID") && retryCount < 100) {
          System.out.println("Retry for XDMP-FORESTNID: " + retryCount);
          i++;
          continue;
        } else if (e.getCode().equals("XDMP-DATABASEDISABLED") && retryCount < 100) {
          System.out.println("Retry for XDMP-DATABASEDISABLED: " + retryCount);
          i++;
          Thread.sleep(5000);
          continue;
        } else if (e.getCode().equals("XDMP-MULTIROOT") && retryCount < 100) {
          System.out.println("JJAMES: Insert failed for XDMP-MULTIROOT: " +
                uniqueURI + ", " + retryCount);
          // need to figure out how not to retry
          i++;
          Thread.sleep(5000);
          continue;
        } else if (e.getCode().equals("XDMP-DOCSTARTTAGCHAR") && retryCount < 100) {
          System.out.println("JJAMES: Insert failed for XDMP-DOCSTARTTAGCHAR: " +
                uniqueURI + ", " + retryCount);
          // need to figure out how not to retry
          i++;
          Thread.sleep(5000);
          continue;
        } else if (e.getCode().equals("XDMP-DOCUNEOF") && retryCount < 100) {
          System.out.println("JJAMES: Insert failed for XDMP-DOCUNEOF: " +
                uniqueURI + ", " + retryCount);
        i++;
        Thread.sleep(5000);
        continue;
        } else if (e.getCode().equals("XDMP-DOCUTF8SEQ") && retryCount < 100) {
          System.out.println("JJAMES: Insert failed for XDMP-DOCUTF8SEQ: " +
                uniqueURI + ", " + retryCount);
          i++;
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
        System.out.println("In the Throwable catch");
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
      // System.out.println("Starting test ");
      // System.out.println(queryTestData.toString());
      init();
      System.out.println(uniqueURI);

      ResultsLogEntry logEntry = null;

      logEntry = new ResultsLogEntry("thread " + threadName +
                                      " SqlQueryTester ");
      logEntry.startTimer();

      for (int i = 0; i < queryTestData.getNumOfLoops() && alive; i++) {

        currentLoop = i;
        updateCounter(i);

        connect();

        uriDelta = Integer.toString(i);
        runQueries();

        if (alive)
          verifyIntervalAfterIteration(i+1);
        disconnect();

      }

      logEntry.stopTimer();
      logEntry.setPassFail(true);
      ResultsLogger logger =
        StressManager.getResultsLogger(queryTestData.getLogFileName());
      if (logger != null)
        logger.logResult(logEntry);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      cleanup();
    }

    // report the results
    System.out.println("Thread " + threadName + " complete:  total queries " + numQueried);

    updateCounter(0);
    setTestName("Finished");

    alive = false;

  }

}
