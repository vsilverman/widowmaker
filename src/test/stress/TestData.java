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

/************************************************************************************************
 * TestData class holds all information about what actions are to be performed by XccStressTester
 *
 *************************************************************************************************/
package test.stress;

import java.io.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * Insert the type's description here.
 */
public class TestData {
  private String testType = null;
  private boolean isBinaryTest = false;
  private boolean isJSONTest = false;
  private boolean isInsertTime = false;
  private int numLoops = 1;
  private int maxSleepTime = 0;
  private String logOption = "";
  private boolean toScreen = true;
  private String dataFileName = "";
  private String logFileName = "";
  private String outputFileName = "";
  protected boolean multiStatement = false;
  protected int batchSize = 1;
  protected boolean rollback = false;
  protected boolean isPerfTest = false;
  protected String temporalCollection = "";
  protected int minRequestTimeLimit = 0;
  protected int maxRequestTimeLimit = 0;

  /**
   * TestData constructor comment.
   */
  public TestData() {
  }

  /**
   * Insert the method's description here. Creation date: (10/10/2000 4:27:54
   * PM)
   * 
   * @param fileName
   *          String
   */
  public TestData(String fileName) throws Exception {

    this.dataFileName = fileName;
    // load the file into a DOM object.
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    File inputFile = new File(fileName);
    if (!inputFile.canRead()) {
      throw new IOException("missing or unreadable inputPath: "
          + inputFile.getCanonicalPath());
    }
    String tmp = "";

    Document testDocument = builder.parse(inputFile);
    NodeList testList = testDocument.getDocumentElement().getChildNodes();
    for (int i = 0; i < testList.getLength(); i++) {
      if (testList.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      String tagName = ((Element) testList.item(i)).getTagName();
      if (tagName.equalsIgnoreCase("testtype")) {
        String perf = ((Element) testList.item(i)).getAttribute("perf");
        if (perf != null) {
          if (perf.equalsIgnoreCase("true")) {
            isPerfTest = true;
          } else {
            isPerfTest = false;
          }
        } else
          isPerfTest = false;

        testType = getNodeText((Element) testList.item(i));
        String binaryTest = ((Element) testList.item(i)).getAttribute("binary");
        if (binaryTest.equalsIgnoreCase("true"))
          isBinaryTest = true;
        String jsonTest = ((Element) testList.item(i)).getAttribute("json");
        if (jsonTest.equalsIgnoreCase("true"))
          isJSONTest = true;

        String insertTime = ((Element) testList.item(i)).getAttribute("inserttime");
        if (insertTime.equalsIgnoreCase("true"))
          isInsertTime = true;
      } else if (tagName.equalsIgnoreCase("numLoops")) {
        tmp = getNodeText((Element) testList.item(i));
        if (!(tmp == null || tmp.equals("")))
          numLoops = Integer.parseInt(tmp);
      } else if (tagName.equalsIgnoreCase("toscreen")) {
        tmp = getNodeText((Element) testList.item(i));
        if (!(tmp == null || tmp.equals("")))
          if (tmp.equalsIgnoreCase("false"))
            toScreen = false;
      } else if (tagName.equalsIgnoreCase("sleeptime")) {
        tmp = getNodeText((Element) testList.item(i));
        if (!(tmp == null || tmp.equals("")))
          maxSleepTime = Integer.parseInt(tmp);
      } else if (tagName.equalsIgnoreCase("logoption")) {
        logOption = getNodeText((Element) testList.item(i));
      } else if (tagName.equalsIgnoreCase("logfilename")) {
        // need to do something for auto...
        logFileName = getNodeText((Element) testList.item(i));
        logFileName = logFileName.replaceAll("QA_HOME",
                        System.getProperty("QA_HOME"));
        logFileName = logFileName.replaceAll("TMP_DIR",
                        System.getProperty("TMP_DIR"));
        // System.err.println("logFileName is " + logFileName);
      } else if (tagName.equalsIgnoreCase("outputfilename")) {
        outputFileName = getNodeText((Element) testList.item(i));
      } else if (tagName.equalsIgnoreCase("request_time_limit")) {
        setRequestTimeLimits((Element) testList.item(i));
      } else if (tagName.equalsIgnoreCase("temporalcollection")) {
        temporalCollection = getNodeText((Element) testList.item(i));
      }
    }

  }

  /**
   * normalizes paths by replacing the common test variables
   * with their environment variable values
   */
  public String expandQaPath(String path) {

    if (path == null)
      return null;

    String str = path;
    str = str.replaceAll("QA_HOME", System.getProperty("QA_HOME"));
    str = str.replaceAll("QA_TESTDATA", System.getProperty("QA_TESTDATA"));
    str = str.replaceAll("TMP_DIR", System.getProperty("TMP_DIR"));

    return str;
  }

  /**
   * Gets the logFileName property (String) value.
   * 
   * @return The logFileName property value.
   */
  public String getLogFileName() {
    return logFileName;
  }

  /**
   * Gets the dataFileName property (String) value.
   * 
   * @return The datalogFileName property value.
   */
  public String getDataFileName() {
    return dataFileName;
  }

  /**
   * Gets the logOption property (String) value.
   * 
   * @return The logOption property value.
   */
  public String getLogOption() {
    return logOption;
  }

  /**
   * Gets the maxSleepTime property (int) value.
   * maxSleepTime is in milliseconds.
   * 
   * @return The maxSleepTime property value.
   */
  public int getMaxSleepTime() {
    return maxSleepTime;
  }

  /**
   * Gets the numOfLoops property (int) value.
   * 
   * @return The numOfLoops property value.
   */
  public int getNumOfLoops() {
    return numLoops;
  }

  /**
   * Gets the outputFileName property (String) value.
   * 
   * @return The outputFileName property value.
   */
  public String getOutputFileName() {
    return outputFileName;
  }

  /**
   * Gets the testcaseID property (String) value.
   * 
   * @return The testcaseID property value.
   */
  public String getTestType() {
    return testType;
  }

  /**
   * Sets the testcaseID property (String) value.
   */
  public void setTestType(String testType) {
    this.testType = testType;
  }

  /**
   * Gets the isBinaryTest property (boolean) value.
   * 
   * @return The isBinaryTest property value.
   */
  public boolean isBinaryTest() {
    return isBinaryTest;
  }
  
  /**
   * Gets the isJSONTest property (boolean) value.
   * 
   * @return The isJSONTest property value.
   */
  public boolean isJSONTest() {
    return isJSONTest;
  }

  /**
   * Gets the isInsertTime property (boolean) value.
   *
   * @return The isInsertTime property value.
   */
  public boolean isInsertTime() {
    return isInsertTime;
  }

  /**
   * Gets the toScreen property (boolean) value.
   * 
   * @return The toScreen property value.
   */
  public boolean getToScreen() {
    return toScreen;
  }

  public void setMultiStatement(String value) {
    if (value.equalsIgnoreCase("true")) {
      multiStatement = true;
    } else
      multiStatement = false;
  }

  public void setMultistatment(boolean isMulti) {
    multiStatement = isMulti;
  }

  public void setBatchSize(int size) {
    batchSize = size;
  }

  public void setRollback(boolean rb) {
    rollback = rb;
  }

  public boolean getRollback() {
    return rollback;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public boolean getMultiStatement() {
    return multiStatement;
  }

  public boolean getIsPerfTest() {
    return isPerfTest;
  }

  public String getTemporalCollection() {
    return temporalCollection;
  }

  public int getMinRequestTimeLimit() {
    return minRequestTimeLimit;
  }

  public int getMaxRequestTimeLimit() {
    return maxRequestTimeLimit;
  }

  protected void setRequestTimeLimits(Node t) {
    String tmp;

    if ((t == null) || (t.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE))
      return;
    NodeList children = t.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      if (children.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      String tagName = ((Element) children.item(i)).getTagName();
      if (tagName.equals("min")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals("")))
          minRequestTimeLimit = Integer.parseInt(tmp);
      }
      else if (tagName.equals("max")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals("")))
          maxRequestTimeLimit = Integer.parseInt(tmp);
      }
    }
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
        text += child.getNodeValue();
    }
    return text;
  }

  /**
   * Insert the method's description here. Creation date: (10/7/2000 11:57:25
   * PM)
   * 
   * @return String
   */
  public String toString() {
    String temp = "#\n";

    temp += "TestcaseID = " + testType + "\n";
    temp += "ThreadID = " + Thread.currentThread().getId() + "\n";
    temp += "BinaryTest = " + isBinaryTest + "\n";
    temp += "JSONTest = " + isJSONTest + "\n";
    temp += "InsertTime = " + isInsertTime + "\n";
    temp += "NumberOfLoops = " + numLoops + "\n";
    temp += "MaxSleepTime = " + maxSleepTime + "\n";
    temp += "LogOption = " + logOption + "\n";
    temp += "WriteToScreen = " + toScreen + "\n";
    temp += "LogFilename = " + logFileName + "\n";
    temp += "OutputFileName = " + outputFileName + "\n";
    temp += "minRequestTimeLimit = " + minRequestTimeLimit + "\n";
    temp += "maxRequestTimeLimit = " + maxRequestTimeLimit + "\n";

    return temp;
  }
}
