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
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;

import test.utilities.BaseTemplateParser;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.exceptions.RetryableXQueryException;
import com.marklogic.xcc.exceptions.XQueryException;
import com.marklogic.xcc.exceptions.ServerConnectionException;

import test.telemetry.TelemetryServer;

public class XccLoadTester extends XccStressTester {
  // include data class here
  protected LoadTestData loadTestData = null;
  protected boolean isLoadDir = false;
  protected int numLoaded = 0;
  protected String uniqueURI = null;
  protected String uriDelta = "";
  protected String randStr = null;
  protected String dateStr = null;
  protected boolean isJSLoad = false; 

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

  public XccLoadTester(ConnectionData connData, LoadTestData loadData,
                       String threadName) {
    super(connData, loadData, threadName);
    loadTestData = loadData;
    if(loadTestData.getLanguage().equals("javascript")) isJSLoad = true; 
    setUniqueURI();
  }

  public XccLoadTester(LoadTestData loadData,
                       String threadName) {
    super(loadData, threadName);
    loadTestData = loadData;
    if(loadTestData.getLanguage().equals("javascript")) isJSLoad = true; 
    setUniqueURI();
  }

  public XccLoadTester() {
  }

  public void initialize(TestData testData, String threadName) {
    loadTestData = (LoadTestData)testData;
    super.initialize(testData, threadName);
  }

  public void init() throws Exception {
    setLoadDir();
    setUniqueURI();
    setTelemetryData();
  }

  protected void setUniqueURI() {
    // avoid URIs that make log file searching miserable
    do {
      randStr = randomString(16);
    } while ((randStr.indexOf("XDMP") > -1) || (randStr.indexOf("SVC") > -1));
    dateStr = getDateString();
    if (uniqueURI == null)
      uniqueURI = "/" + randStr + "/" + dateStr + "/" + threadName + "/";

    dateStr += threadName;
  }

  private String telemetryCounterString = null;
  private String telemetryTestName = null;
  private String telemetryTestString = null;
  private String telemetryStartString = null;
  private String telemetryFileNameString = null;

  /**
   * should be called only by init
   */
  protected void setTelemetryData() {
    telemetryCounterString = "testthread." + threadName + ".counter";
    telemetryTestString = "testthread." + threadName + ".testname";
    telemetryStartString = "testthread." + threadName + ".starttime";
    telemetryFileNameString = "testthread." + threadName + ".testfile";
    if (telemetryTestName == null)
      setTestName("unknown");
    TelemetryServer telemetry = StressManager.getTelemetryServer();
    if (telemetry != null) {
      telemetry.sendTelemetry(telemetryCounterString, "0");
      telemetry.sendTelemetry(telemetryTestString, telemetryTestName);
      Date d = new Date();
      telemetry.sendTelemetry(telemetryStartString, d.toString());
      String str = loadTestData.getDataFileName();
      if (str != null && str.length() > 0) {
        File f = new File(str);
        telemetry.sendTelemetry(telemetryFileNameString, f.getName());
      }
    }
  }

  protected void setTestName(String s) {
    telemetryTestName = s;
    TelemetryServer telemetry = StressManager.getTelemetryServer();
    if ((telemetry != null) && (telemetryTestString != null)) {
      telemetry.sendTelemetry(telemetryTestString, telemetryTestName);
      String str = loadTestData.getDataFileName();
      if (str != null && str.length() > 0)
        telemetry.sendTelemetry(telemetryFileNameString, str);
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
    if (loadTestData.isInsertTime()) 
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
      if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG"))
        s.runQuery(xqStr);

      serverIndex++;
    }
  }
  
  protected void verifyIntervalAfterIteration(int loop) throws Exception {
    verifyInterval();
  }

  protected void setLoadDir() {
    if (!loadTestData.getLoadDir().equals(""))
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
  private ContentCreateOptions getLoadOptions() throws Exception{
	ContentCreateOptions options = null; 
	if (loadTestData.isJSONTest()) {
	  options = new ContentCreateOptions();
	  options.setFormat(DocumentFormat.JSON);
	  setTransactionTimeout(3600);
	} else if (!loadTestData.isBinaryTest()) {
	  options = ContentCreateOptions.newXmlInstance();
          setTransactionTimeout(3600)  ;
	} else{
	  options = ContentCreateOptions.newBinaryInstance();
	}
	String[] collections = { uniqueURI };
	options.setCollections(collections);

	String tempcoll = loadTestData.getTemporalCollection();
	if (tempcoll!=null && tempcoll.length()!=0) options.setTemporalCollection(tempcoll);

	return options; 
  }
  protected String getTimeContent(File file){
	final Date d = new Date();
	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	df.setTimeZone(TimeZone.getTimeZone("GMT"));
	StringBuilder buf = new StringBuilder();
	try {
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		String sCurrentLine;
		int lineno = 1;
		while((sCurrentLine = br.readLine()) != null) {
		  if (lineno == 2) {
		  buf.append("<insert-time>");
		  buf.append("Z</insert-time>\n");
		  }
		  buf.append(sCurrentLine);
		  buf.append("\n");
		  lineno++;
		}
		br.close();
		fr.close();
	}catch (Exception e) {
	     ;
	}
	return buf.toString(); 
  }
  protected String getTemplateContent(File file) throws Exception{
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	try{
		String fullpath = loadTestData.getLoadDir() + File.separator +
                          file.getName();
		FileInputStream fis = new FileInputStream(fullpath);
		BaseTemplateParser parser = new BaseTemplateParser();
		parser.setFieldManager(loadTestData.fieldManager);
		parser.initialize();
		parser.parseTemplate(fis, baos);
		fis.close();
		fis = null;
		parser.cleanup();
	}catch (Exception e) {
             ;
        }
	return baos.toString("UTF-8");
  }
  protected void loadContentFromDir(boolean rollback)
      throws InterruptedException {
    File[] fileList = listFiles(loadTestData.getLoadDir());
    int curBatch = 0;
    int batchStart = 0;
    int retryCount = 0;
    String verificationQuery = "";
    int i = 0;

    ResultsLogEntry logEntry = null;

    while (i < fileList.length && alive) {
      boolean inserted = false;
      try {
        if (curBatch == 0) {
          beginTransaction();
          batchStart = i;
        }

        ContentCreateOptions options = getLoadOptions();
        String curURI = uniqueURI + uriDelta + fileList[i].getName();

    Content content;
    if (loadTestData.isInsertTime()) {
      content = ContentFactory.newContent(curURI, getTimeContent(fileList[i]), options);
    }else if(loadTestData.isTemplate()){
      content = ContentFactory.newContent(curURI, getTemplateContent(fileList[i]), options);
    }else {
      content = ContentFactory.newContent(curURI, fileList[i], options);
    }

    if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG")) {
      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      Date date = new Date();
      System.out.println("Loading " + curURI + " at " + dateFormat.format(date));
    }
    for (SessionHolder s : sessions) {
      String hostName = s.session.getConnectionUri().getHost();
      logEntry = new ResultsLogEntry("thread " + threadName + " host " + hostName + " URI " + curURI);
      logEntry.startTimer();
      long startTime = System.currentTimeMillis();
      //do js load here
      if(! isJSLoad){  
        s.session.insertContent(content);
      }else{
        jsLoad(fileList[i].getAbsolutePath(), curURI, s); 
      } 

      logEntry.setPassFail(true);
      logEntry.stopTimer();
      ResultsLogger logger = StressManager.getResultsLogger(loadTestData.getLogFileName());
      if (logger != null) {
        logger.logResult(logEntry);
      }

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
      curBatch = 0; retryCount = 0;
    }
    // throttles the work contributed by this thread
    Thread.sleep(loadTestData.getMaxSleepTime());

    // check documents in db at interval
    // if multistmt have to check at batch end
    if (numLoaded % loadTestData.getCheckInterval() == 0 && curBatch == 0
        && !rollback)
      verifyInterval();

    ++i;
    }catch (RetryableXQueryException e) {
        try {
	  handleLoadException(e); 
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
      } catch (Exception e) {
        handleLoadException(e); 
        e.printStackTrace();
        try {
          rollbackTransaction();
        } catch (Exception Ei) {
        }
        ++i; curBatch = 0; retryCount = 0; totalTime = 0;
      }
    }
  }
  private void handleLoadException(Exception e){
	try{
		if( e instanceof ServerConnectionException){
		   System.out.println("Retry for ServerConnectionException: " + e.getMessage()); 
		   Thread.sleep(10000); 
		}else if( e instanceof XQueryException){
		   //write an XQueryException handler this one is involved
		   handleLoadXQueryException((XQueryException) e); 
		}
	}catch(Exception e2){
		e2.printStackTrace(); 
	}	
  }
  private void handleLoadXQueryException(XQueryException e){
	if (e.getCode().equals("XDMP-FORESTNID")){
		System.out.println("Retry for XDMP-FORESTNID: "); 
	}
	else if(e.getCode().equals("XDMP-DATABASEDISABLED")){
		System.out.println("Retry for XDMP-DATABASEDISABLED: "); 
	}
	else if((e.getCode().equals("XDMP-EXTIME") || (e.getCode().equals("SVC-EXTIME")))){
		System.out.println("Retry for EXTIME: "); 
	}
	else if(e.getCode().equals("XDMP-MULTIROOT")){
		System.out.println("JJAMES: Insert failed for XDMP-MULTIROOT: " +
            uniqueURI ); 
	}
	else if(e.getCode().equals("XDMP-DOCSTARTTAGCHAR")){
		System.out.println("JJAMES: Insert failed for XDMP-DOCSTARTTAGCHAR: " +
                uniqueURI); 
	}
	else if(e.getCode().equals("XDMP-DOCUNEOF")){
		System.out.println("JJAMES: Insert failed for XDMP-DOCUNEOF: " +
                uniqueURI); 
	}
	else if(e.getCode().equals("XDMP-DOCUTF8SEQ")){
		System.out.println("JJAMES: Insert failed for XDMP-DOCUTF8SEQ: " +
                uniqueURI); 
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
      for (int i = 0; i < loadTestData.getNumOfLoops() && alive; i++) {
        connect();
        updateCounter(i);
        if (isLoadDir) {
          uriDelta = Integer.toString(i);
          loadContentFromDir(loadTestData.getRollback());
        } else if (loadTestData.getGenerateQuery().length() != 0) {
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

    updateCounter(0);
    setTestName("Finished");

    alive = false;

  }
}
