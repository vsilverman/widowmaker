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
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.ArrayList;
import java.util.Iterator;
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
import test.utilities.BaseTemplateParser;

import test.utilities.MarkLogicWrapper;
import test.utilities.MarkLogicWrapperFactory;

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

import com.hp.hpl.jena.graph.Triple;

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



public class TemplateDataUpdateTester extends XccLoadTester
    implements LoadableStressTester {
  public static boolean SKIP_VERIFICATION_DEFAULT = true;

  public static final boolean DEBUG_TURTLE = false;

  // include data class here
  protected TemplateLoadTestData loadTestData = null;
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
  private HashMap<String, UpdateStatusRecord> updateList = null;
  protected String tmpdir = null;
  DateFormat dateFormat = null;
  protected int currentLoop = 0;

  protected String templateFile;

  protected boolean skipVerification =  SKIP_VERIFICATION_DEFAULT;

  protected TrackingDataManager tracker = null;
  protected StatusUpdateField statusUpdateField = null;
  protected StatusUpdateField statusUriField = null;

  boolean doOneUpdateAtaTime = true;

  public String getTestType() {
    return "TdeUpdateTester";
  }

  public TestData getTestDataInstance(String filename)
      throws Exception {
    return new TemplateLoadTestData(filename);
  }

  public TemplateDataUpdateTester(ConnectionData connData, TemplateLoadTestData loadData,
                       String threadName) {
    super(connData, loadData, threadName);
    setTestName("TemplateDataUpdateTester");
    loadTestData = loadData;
    randNum = new Random();
    updateList = new HashMap<String, UpdateStatusRecord>();

    templateFile = loadTestData.getLoadDir() + File.separator + "UpdateTrans.xml";

    setUniqueURI();
  }

  public TemplateDataUpdateTester() {
    randNum = new Random();
    updateList = new HashMap<String, UpdateStatusRecord>();
  }

  public void initialize(TestData testData, String threadName) {
    loadTestData = (TemplateLoadTestData)testData;
    super.initialize(testData, threadName);

    templateFile = loadTestData.getLoadDir() + File.separator + "UpdateTrans.xml";
    setUniqueURI();
  }

  public void init() throws Exception {
    setTestName("TemplateDataUpdateTester");
    setLoadDir();
    setUniqueURI();
    setTelemetryData();
    setPrivateTelemetryData();
    setParseFieldData();

    dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    tracker = TrackingDataManager.getTracker();
  }

  protected void setUniqueURI() {
    randStr = randomString(16);
    dateStr = getDateString();
    if (uniqueURI == null)
      uniqueURI = "/" + randStr + "/" + dateStr + "/" + threadName + "/";

    dateStr += threadName;
  }

  class TradeReferenceID {
    String ID;
    TradeReferenceID(String ID) {
      this.ID = ID;
    }

    String getID() {
      return ID;
    }

    void setID(String ID) {
      this.ID = ID;
    }
  }

  class TradeReferenceIDField
          extends BaseParseField {
      TradeReferenceID id = null;

    TradeReferenceIDField(TradeReferenceID id) {
      this.id = id;
    }

    public void initialize(ParseFieldManager mgr, Node t) {
      // do nothing here - this is fudged
    }

    public String generateData(String token) {
      return id.getID();
    }

    public String generateData(String token, HashMap extra) {
      return generateData(token);
    }

  }

  public static final String PRIMARY_URI_FIELD = "__PRIMARY_URI__";
  public static final String NEW_STATUS_FIELD = "__NEW_STATUS__";

  class StatusUpdateField
          extends BaseParseField {
      UpdateStatusRecord record = null;

    StatusUpdateField(UpdateStatusRecord record, String token) {
      this.record = record;
      this.token = token;
    }

    public void initialize(ParseFieldManager mgr, Node t) {
      // do nothing here - this is fudged
    }

    public void setStatusRecord(UpdateStatusRecord record) {
      this.record = record;
    }

    public String generateData(String token) {

      // System.out.println("generateData:  token is " + token);
      if (record == null) {
        System.out.println("generateData:  record is null");
        Throwable t = new Throwable("generateData:  null");
        t.printStackTrace();
      } else {
        // System.out.println("generateData:  record is " + record.toString());
      }

      if (record != null) {
        if (token.equals(PRIMARY_URI_FIELD))
          return record.primaryUri;
        if (token.equals(NEW_STATUS_FIELD)) {
          String newStatus = getNextStatus(record.currentStatus);
          if (newStatus == null) {
            System.out.println("generateData:  about to return a null newStatus");
          }
          return newStatus;
        }
      }
      return "ERROR_MISSING_INFORMATION";
    }

    public String generateData(String token, HashMap extra) {
      return generateData(token);
    }

  }

  protected void setParseFieldData() {

    String fieldName = PRIMARY_URI_FIELD;
    statusUpdateField = new StatusUpdateField(null, fieldName);
    loadTestData.fieldManager.addCustomField(fieldName, statusUpdateField);

    fieldName = NEW_STATUS_FIELD;
    statusUriField = new StatusUpdateField(null, fieldName);
    loadTestData.fieldManager.addCustomField(fieldName, statusUriField);
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

    // now I have the root node, let's get to work
    // NodeList nodeList = 

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

  /**
   * does nothing right now - figure it out
   */
  protected void verifyCounts() {
  }


  protected void verifyIntervalAfterIteration(int loop) throws Exception {

    if ((loop % 100) == 0)
      verifyCounts();
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


  /**
   * preamble work for any bulk that's being done
   */
  protected void bulkPreProcess() {

    int count = updateList.size();

    ResultsLogEntry logEntry = new ResultsLogEntry("thread " + threadName + 
                                    " TemplateDataUpdateTester " +
                                    " bulkPreProcess " + 
                                    " count " + count);
    logEntry.startTimer();

    tmpdir = "/tmp/" + randomString(16);
    File dir = new File(tmpdir);
    if (dir.exists()) {
      System.err.println("Error:  bulkPreProcess finds directory exists:  " + tmpdir);
    }

    dir.mkdir();

/*
    loadTestData.host = "rh7-intel64-80-qa-dev-5";
    loadTestData.port = 8020;
    loadTestData.user = "admin";
    loadTestData.password = "admin";
*/

    logEntry.stopTimer();
    logEntry.setPassFail(true);
    ResultsLogger logger =
      StressManager.getResultsLogger(loadTestData.getLogFileName());
    logger.logResult(logEntry);

  }

  /**
   * decide how many documents we should update
   */
  protected long getWorkCount() {

    long count = 0;

    // get the count from the response
    count = tracker.getWorkCount();

    // System.out.println("getWorkCount:  tracker returned " + count);

    return count;
  }

  /**
   * decide which documents to update
   * return the limit we set (work around a bug in Jena????)
   */
  protected int generateWorkload()
    throws Exception {

    ResultsLogEntry logEntry = null;

    logEntry = new ResultsLogEntry("thread " + threadName + 
                                    " TemplateDataUpdateTester " +
                                    " currentLoop " + currentLoop +
                                    " generateWorkload ");
    logEntry.startTimer();


    long count = getWorkCount();
    int limit = 1;

    // decide on the threshold
    if (count < 1000) {
      limit = 1;
    } else if (count < 10000) {
      limit = 100;
    } else if (count < 100000) {
      limit = 500;
    } else if (count < 1000000) {
      limit = 1000;
    } else if (count < 5000000) {
      limit = 2000;
    } else if (count < 7000000) {
      limit = 2500;
    } else {
      limit = 3500;
    }

    // limit = 10;

    // System.out.println("generateWorkload:  limit is set to " + limit);

    updateList = tracker.generateWorkload(limit);
    Iterator iter = updateList.values().iterator();
    while (iter.hasNext()) {
      UpdateStatusRecord record = (UpdateStatusRecord)iter.next();
      // System.out.println("record:  " + record.toString());
    }

    count = updateList.size();

    logEntry.stopTimer();
    logEntry.setPassFail(true);
    logEntry.setInfo(" count " + count);
    ResultsLogger logger =
      StressManager.getResultsLogger(loadTestData.getLogFileName());
    logger.logResult(logEntry);

    return limit;
  }

  protected void processWorkload() {

    // System.out.println("TemplateDataUploadTester.processWorkload");

    long count = updateList.size();

    ResultsLogEntry logEntry = new ResultsLogEntry("thread " + threadName + 
                                    " TemplateDataUpdateTester " +
                                    " processWorkload " + 
                                    " count " + count);
    logEntry.startTimer();

    int counter = 0;
    Iterator iter = updateList.values().iterator();
    try {
      while (iter.hasNext()) {
        UpdateStatusRecord record = (UpdateStatusRecord)iter.next();

        // TODO: we need to refresh here to help avoid problems
        {
          try {
          UpdateStatusRecord newRecord = tracker.getOneRecord(record.primaryUri);
          if (!newRecord.currentStatus.equals(record.currentStatus)) {
/*
            System.out.println("currentStatus has changed after initial fetch:  "
                                      + record.primaryUri + ", "
                                      + record.currentStatus + ", "
                                      + newRecord.currentStatus);
*/
            record.currentStatus = newRecord.currentStatus;
          }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }

        ByteArrayOutputStream baos = generateOneDocument(record.primaryUri, record);
        String junk = baos.toString("UTF-8");
        ByteArrayInputStream bais = new ByteArrayInputStream(junk.getBytes());

        sendOneDocument(bais);

        String txInstant = getElementValue(junk, "dbts:TxInstant");
        if (txInstant != null)
          record.setLastDate(txInstant);

        // TODO:  this needs to be where we do the update for the backing data
        if (doOneUpdateAtaTime)
          updateBackingDataStatus(record);

        updateCounter(++counter);
      }
    }
    catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }

    logEntry.stopTimer();
    logEntry.setPassFail(true);
    ResultsLogger logger =
      StressManager.getResultsLogger(loadTestData.getLogFileName());
    logger.logResult(logEntry);

  }

  /**
   * make one document for the passed in URI and status
   */
  protected ByteArrayOutputStream generateOneDocument(String uri, UpdateStatusRecord record) {

    if ((uri == null) || (uri.length() == 0) || (record == null))
      return null;

    // System.out.println("generateOneDocument:  record is " + record.toString());

    if (statusUpdateField != null) {
      statusUpdateField.setStatusRecord(record);
    } else {
      System.out.println("STATUS UPDATE FIELD IS NULL");
    }
    if (statusUriField != null) {
      statusUriField.setStatusRecord(record);
    } else {
      System.out.println("STATUS URI UPDATE FIELD IS NULL");
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    File f = null;
    FileInputStream fis = null;

    try {

      f = new File(templateFile);
      if (!f.exists()) {
        System.out.println("template file does not exist:  " + templateFile);
        return null;
      }
      fis = new FileInputStream(f);
      BaseTemplateParser parser = new BaseTemplateParser();
      parser.setFieldManager(loadTestData.fieldManager);

      // create a new parse field that has a handle to the update status record
      // this allows us to look at the current status and then move to the next one

      // now parse it
      parser.parseTemplate(fis, baos);

      fis.close();

      parser.cleanup();
    }
    catch (FileNotFoundException e) {
      System.out.println("template file is missing:  " + templateFile);
      e.printStackTrace();
    }
    catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (fis != null)
          fis.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return baos;
  }

  /**
   * send one document to the server, or put it somewhere for a bulk operation
   */
  protected void sendOneDocument(InputStream is) {
    // do bulk here for now
    byte[] bytes = new byte[1024];

    String fname = randomString(16) + ".xml";
    File f = null;
    File tmp = new File(tmpdir);
    FileOutputStream fos = null;
    BufferedInputStream bis = null;
    BufferedOutputStream bos = null;

    // System.out.println("sendOneDocument:  creating file " + fname);

    try {
      f = new File(tmp, fname);
      fos = new FileOutputStream(f);
      bis = new BufferedInputStream(is);
      bos = new BufferedOutputStream(fos);
      int bytesRead = 0;
      while (bytesRead != -1) {
        bytesRead = bis.read(bytes) ;
        if (bytesRead > 0)
          bos.write(bytes, 0, bytesRead);
      }
      bos.flush();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (bos != null)
          bos.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

  /**
   * a no-op for immediate transactions, this allows any way to do a bulk
   * operation for all the documents that have been generated
   */
  protected void doBulk() {

    // what if, as a temporary measure, we just read the files and send them over
    // one at a time through XCC?

    // System.out.println("TemplateDataUploadTester.doBulk");

    ResultsLogEntry logEntry = null;

    int count = updateList.size();

    logEntry = new ResultsLogEntry("thread " + threadName + 
                                    " TemplateDataUpdateTester " +
                                    " doBulk " + 
                                    " count " + count);
    logEntry.startTimer();

    File tmpDir = new File(tmpdir);

    int counter = 0;

    if(tmpDir.exists()) {
      if(tmpDir.isDirectory()) {
        String[] strFiles = tmpDir.list();
        for(String strFilename: strFiles) {
          File fileToSend = new File(tmpDir, strFilename);
          // this could be recursive just by calling back in to ourselves here
          String fullname = fileToSend.getAbsolutePath();
          try {
            sendOneFile(fullname, false);
            updateCounter(++counter);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    } else {
      // nothing to do
    }
    logEntry.stopTimer();
    logEntry.setPassFail(true);
    ResultsLogger logger =
      StressManager.getResultsLogger(loadTestData.getLogFileName());
    logger.logResult(logEntry);

  }

  protected String getNextStatus(String currentStatus) {

    String[] statuses = { "VALID", "FILED", "TRANSACTED", "FULFILLED", "SETTLED" };

    int index = -1;
    for (int ii = 0 ; ii < statuses.length; ii++ ) {
      if (statuses[ii].equals(currentStatus)) {
        index = ii;
      }
    }

    if (index == -1)
      return null;
    if (++index >= statuses.length) {
      System.out.println("I think this is the problem: currentStatus " + currentStatus + " wants to become null");
      // return null;
      System.out.println("Forcing us back to the final status");
      --index;
    }

    return statuses[index];
  }

  protected void updateBackingDataStatus(UpdateStatusRecord record) {


    String newStatus = getNextStatus(record.currentStatus);
    record.newStatus = newStatus;

/*
    if (doOneUpdateAtaTime) {
      tracker.updateStatusData(record, newStatus);
    }
*/
    tracker.updateStatusData(record, newStatus);

  }

  /**
   * perform whatever needs to be done to update our notion of the
   * status of the document
   */
  protected void updateCurrentStatus() {

    // System.out.println("TemplateDataUploadTester.updateCurrentStatus");

    ResultsLogEntry logEntry = null;

    int count = updateList.size();

    logEntry = new ResultsLogEntry("thread " + threadName + 
                                    " TemplateDataUpdateTester " +
                                    " updateCurrentStatus " + 
                                    " count " + count);
    logEntry.startTimer();

    // update all the statuses with new information
    
    // playing with how to do these
    // boolean doOneUpdateAtaTime = true;

    int counter = 0;

    try {

      Iterator iter = updateList.values().iterator();
      while (iter.hasNext()) {
        UpdateStatusRecord record = (UpdateStatusRecord)iter.next();

        // updateBackingDataStatus(record);

/*
        String newStatus = getNextStatus(record.currentStatus);
        record.newStatus = newStatus;

        if (doOneUpdateAtaTime) {
          tracker.updateStatusData(record, newStatus);
        }
*/

        updateCounter(++counter);
      }

      if (!doOneUpdateAtaTime) {
        // done as bulk - experiment
        tracker.updateStatusData(updateList);
      }

      logEntry.setPassFail(true);
    }
    catch (Exception e) {
      e.printStackTrace();
      logEntry.setPassFail(false);
    }


    logEntry.stopTimer();
    ResultsLogger logger =
      StressManager.getResultsLogger(loadTestData.getLogFileName());
    logger.logResult(logEntry);

  }

  /**
   * perform any tidying up that needs to be done
   */
  protected void postProcess() {

    // System.out.println("TemplateDataUploadTester.postProcess");

    int count = updateList.size();

    ResultsLogEntry logEntry = new ResultsLogEntry("thread " + threadName + 
                                    " TemplateDataUpdateTester " +
                                    " postProcess " + 
                                    " count " + count);
    logEntry.startTimer();

    // do your work here

    int counter = 0;


    logEntry.stopTimer();
    logEntry.setPassFail(true);
    ResultsLogger logger =
      StressManager.getResultsLogger(loadTestData.getLogFileName());
    logger.logResult(logEntry);

  }

  /**
   * perform any tidying up that needs to be done
   */
  protected void performVerification() {

    // System.out.println("TemplateDataUploadTester.performVerification");

    int count = updateList.size();

    ResultsLogEntry logEntry = new ResultsLogEntry("thread " + threadName + 
                                    " TemplateDataUpdateTester " +
                                    " performVerification " + 
                                    " count " + count);
    logEntry.startTimer();

    // do your work here

    int counter = 0;

    Iterator iter = updateList.values().iterator();
    try {
      while (iter.hasNext()) {
        UpdateStatusRecord record = (UpdateStatusRecord)iter.next();

        if (counter % 100 == 0) {
          verifyStatus(record, false);
        }

        updateCounter(++counter);
      }
      // done as bulk - experiment
      // tracker.updateStatusData(updateList);
    }
    catch (Exception e) {
      e.printStackTrace();
    }

    logEntry.stopTimer();
    logEntry.setPassFail(true);
    ResultsLogger logger =
      StressManager.getResultsLogger(loadTestData.getLogFileName());
    logger.logResult(logEntry);

  }

  protected void initProcessing() {

    // System.out.println("TemplateDataUploadTester.initProcessing");

    ResultsLogEntry logEntry = null;

    int count = updateList.size();

    logEntry = new ResultsLogEntry("thread " + threadName + 
                                    " TemplateDataUpdateTester " +
                                    " initProcessing " + 
                                    " count " + count);
    logEntry.startTimer();

    // do work here


    logEntry.stopTimer();
    logEntry.setPassFail(true);
    ResultsLogger logger =
      StressManager.getResultsLogger(loadTestData.getLogFileName());
    logger.logResult(logEntry);

  }

  protected void cleanupProcessing() {

    // System.out.println("TemplateDataUploadTester.cleanupProcessing");

    ResultsLogEntry logEntry = null;

    int count = updateList.size();

    logEntry = new ResultsLogEntry("thread " + threadName + 
                                    " TemplateDataUpdateTester " +
                                    " cleanupProcessing " + 
                                    " count " + count);
    logEntry.startTimer();

    // delete all files in the tmp directory

    boolean doDelete = true;

    if (doDelete) {
      File tmpDir = new File(tmpdir);

      if(tmpDir.exists()) {
        if(tmpDir.isDirectory()) {
          if(tmpDir.list().length == 0) {
            tmpDir.delete();
          } else {
            String[] strFiles = tmpDir.list();
            for(String strFilename: strFiles) {
              File fileToDelete = new File(tmpDir, strFilename);
              // this could be recursive just by calling back in to ourselves here
              // System.out.println("deleting file " + fileToDelete.getAbsolutePath());
              boolean bval = fileToDelete.delete();
              if (bval == false)
                System.out.println("delete failed:  " + fileToDelete.getAbsolutePath());
            }
            boolean dval = tmpDir.delete();
            if (dval == false)
              System.out.println("dir delete failed:  " + tmpDir.getAbsolutePath());
          }
        } else {
          tmpDir.delete();
        }
      } else {
        System.out.println("tmpDir does not exist: " + tmpDir.getAbsolutePath());
      }
    }

    logEntry.stopTimer();
    logEntry.setPassFail(true);
    ResultsLogger logger =
      StressManager.getResultsLogger(loadTestData.getLogFileName());
    logger.logResult(logEntry);

  }

  void processUpdates() {

    int limit = -1;

    initProcessing();

    bulkPreProcess();

    try {
      limit = generateWorkload();
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    processWorkload();

    doBulk();

    postProcess();

    updateCurrentStatus();

    performVerification();

    cleanupProcessing();

  }


  /**
   * First attempt at modularizing this operation
   */
  protected void sendOneFile(String filename, boolean rollback)
      throws InterruptedException {

    File[] fileList = new File[1];
    fileList[0] = new File(filename);

    int curBatch = 0;
    int batchStart = 0;
    int retryCount = 0;
    String verificationQuery = "";
    String curURI = null;

    int i = 0;
    while (i < fileList.length && alive) {
      boolean inserted = false;
      try {
        if (curBatch == 0) {
          beginTransaction();
          batchStart = i;
        }

        ContentCreateOptions options = null;
        options = ContentCreateOptions.newXmlInstance();
        options.setFormatXml();
        String[] collections = { uniqueURI };
        options.setCollections(collections);
        curURI = uniqueURI + uriDelta + fileList[i].getName();
        String templateName = fileList[i].getName();

        // System.out.println("processing template " + templateName);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        String fullpath = loadTestData.getLoadDir() + File.separator +
                          fileList[i].getName();

        FileInputStream fis = new FileInputStream(filename);

        // copy the file into a buffer so we can send it over
        byte[] bytes = new byte[1024];
        int bytesRead = 0;
        BufferedInputStream bis = new BufferedInputStream(fis);
        BufferedOutputStream bos = new BufferedOutputStream(baos);
        while (bytesRead != -1) {
          bytesRead = bis.read(bytes);
          if (bytesRead > 0)
            bos.write(bytes, 0, bytesRead);
        }
        bos.flush();
        fis.close();
        String tradeXml = baos.toString("UTF-8");

        if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG")) {
          Date date = new Date();
          System.out.println("Loading " + curURI + " at " + dateFormat.format(date));
        }

        String myUri = getElementValue(tradeXml, "dbts:PmryURI");
        String myStatus = getElementValue(tradeXml, "dbts:Sts");
        // System.out.println("getElementValue:  " + myUri);

        ResultsLogEntry logEntry = null;
        String returnedURI = null;
        String primaryUri = null;

        for (SessionHolder s : sessions) {
          long startTime = System.currentTimeMillis();
        
          logEntry = new ResultsLogEntry("thread " + threadName + 
                                          " PmryURI " + myUri +
                                          " NewStatus " + myStatus);

                                          // " update_" + curURI;

          logEntry.startTimer();

          // THIS IS WHERE WE SEND THE DOCUMENT OVER
          //
          //  s.session.insertContent(content);
          //  s.runQuery(myQueryGoesHere);
          //
          Request request = s.session.newModuleInvoke("/stress/tde-document-submitter.xqy");
          // declare variable $mydoc as xs:string external
          // $mydoc as xs:string now exists
          request.setNewVariable("mydoc", ValueType.XS_STRING, tradeXml);
          request.setNewVariable("myuri", ValueType.XS_STRING, curURI);
          request.setNewVariable("xpath", ValueType.XS_STRING, "dbts:PmryURI");
          ResultSequence rs = s.session.submitRequest(request);

          ResultItem rsItem = rs.resultItemAt(0);
          XdmItem item = rsItem.getItem();
          returnedURI = item.asString();
          rsItem = rs.resultItemAt(1);
          item = rsItem.getItem();
          primaryUri = item.asString();

          // System.out.println("returnedURI, primaryUri = " + returnedURI + ", " + primaryUri);

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
        String error = "ERROR could not load URI : " + curURI;
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
          if (inserted) {
            i++;
            curBatch = 0;
          }
          Thread.sleep(5000);
          continue;
        } else if (e.getCode().equals("XDMP-MULTIROOT") && retryCount < 100) {
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


  /**
   * Second attempt at modularizing this operation
   */
  protected void verifyStatus(UpdateStatusRecord record, boolean rollback)
      throws InterruptedException {

    int curBatch = 0;
    int batchStart = 0;
    int retryCount = 0;
    String verificationQuery = "";
    String curURI = null;

    // don't know if this is needed
    String predicate = "";

    // TODO:  only here until I peel out the breaks in here
    int maxCount = 1;
    int i = 0;
    while ((i < maxCount) && alive) {
      boolean inserted = false;
      try {
        if (curBatch == 0) {
          beginTransaction();
          batchStart = i;
        }

        ContentCreateOptions options = null;
        options = ContentCreateOptions.newXmlInstance();
        options.setFormatXml();

        // System.out.println("processing template " + templateName);

        if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG")) {
          Date date = new Date();
          System.out.println("Loading " + curURI + " at " + dateFormat.format(date));
        }

        String myUri = record.primaryUri;
        String myPredicate = predicate;
        String myStatus = getNextStatus(record.currentStatus);
        if (record.newStatus == null)
          record.newStatus = myStatus;
        // System.out.println("getElementValue:  " + myUri);

        if (myStatus == null) {
          System.out.println("TemplateDataUpdateTester:  myStatus is null");
          System.out.println("we'll get trouble for this:");
          System.out.println("UpdateStatusRecord:  ");
          System.out.println(record);
        }

        ResultsLogEntry logEntry = null;
        String returnedTurtle = null;
        String returnedStatus = null;
        String returnedDate = null;

        for (SessionHolder s : sessions) {
          long startTime = System.currentTimeMillis();
        
          logEntry = new ResultsLogEntry("thread " + threadName + 
                                          " PmryURI " + myUri +
                                          " VerifyStatus " + myStatus);

                                          // " update_" + curURI;

          logEntry.startTimer();

          // THIS IS WHERE WE SEND THE DOCUMENT OVER
          //
          //  s.session.insertContent(content);
          //  s.runQuery(myQueryGoesHere);
          //
          Request request = s.session.newModuleInvoke("/stress/tde-status-validation.xqy");
          // declare variable $mydoc as xs:string external
          // $mydoc as xs:string now exists
          request.setNewVariable("myuri", ValueType.XS_STRING, record.primaryUri);
          request.setNewVariable("pred1", ValueType.XS_STRING, myPredicate);
          request.setNewVariable("status", ValueType.XS_STRING, myStatus);

          ResultSequence rs = s.session.submitRequest(request);

          ResultItem rsItem = rs.resultItemAt(0);
          XdmItem item = rsItem.getItem();
          returnedTurtle = item.asString();
/*
          rsItem = rs.resultItemAt(1);
          item = rsItem.getItem();
          returnedStatus = item.asString();
          rsItem = rs.resultItemAt(2);
          item = rsItem.getItem();
          returnedDate = item.asString();
*/

          if (DEBUG_TURTLE) {
            System.out.println("returnedTurtle = "
                                  + returnedTurtle);
          }

          TemplateDataStatusValidator validator =
              new TemplateDataStatusValidator(StressManager.getValidationManager(),
                                returnedTurtle,
                                record);
          validator.setLogEntry(logEntry);
          validator.setLogFileName(loadTestData.getLogFileName());


          StressManager.validationMgr.validateThis(validator);

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
            || i + 1 == maxCount) {
          if (!rollback) {
            commitTransaction();
            numLoaded += curBatch;
            // verify all documents from this commit are loaded
            for (int pos = i; curBatch > 0; --pos) {
              --curBatch;
            }
          } else {
            rollbackTransaction();
            // verify all documents gone b/c rollback
            for (int pos = i; curBatch > 0; --pos) {
              --curBatch;
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
        String error = "ERROR could not load URI : " + curURI;
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
      for (int i = 0; i < loadTestData.getNumOfLoops() && alive; i++) {

        ResultsLogEntry logEntry = null;

        currentLoop = i;
        updateCounter(i);

        logEntry = new ResultsLogEntry("thread " + threadName + 
                                        " TemplateDataUpdateTester " +
                                        " currentLoop " + currentLoop);
        logEntry.startTimer();

        connect();

        uriDelta = Integer.toString(i);

        processUpdates();

        if (alive)
          verifyIntervalAfterIteration(i+1);
        disconnect();

        logEntry.stopTimer();
        logEntry.setPassFail(true);
        ResultsLogger logger =
          StressManager.getResultsLogger(loadTestData.getLogFileName());
        logger.logResult(logEntry);

      }

      // now that we're through, try the duplicates
/*
      connect();
      processDuplicates();
      disconnect();
*/

      // now try the version increments
/*
      connect();
      processVersionIncrements();
      disconnect();
*/

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
