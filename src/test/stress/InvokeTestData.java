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

/************************************************************************************************
 * InvokeTestData class holds all information about what actions are to be performed by XccStressTester
 *
 *************************************************************************************************/
package test.stress;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Insert the type's description here.
 */
public class InvokeTestData extends TestData {
  protected int numCreate = 1;
  protected int checkInterval = 1;
  protected String loadDir = "";
  protected String createModule = "";
  protected String moduleName = null;
  protected String responseFormat = null;
  // need to create some class that just generates an xml document
  // autoGenerate should be horizon 2
  protected boolean autoGenerate = true;

  // generate queryies must use REPLACE_URI and REPLACE_COLL so that harness can
  // utilize
  // its own naming conventions for document URI's and collections
  protected String generateQuery = "";
  // numGenerated is an attr of generateQuery it is how many documents
  // generated per iteration of the query
  protected int numGenerated = 0;
  protected String language = "xquery"; 

  // transType must be commit or rollback
  /**
   * Insert the method's description here.
   * 
   * @param fileName
   *          String
   */
  public InvokeTestData(String fileName) throws Exception {
    super(fileName);

    // load the file into a DOM object.
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    File inputFile = new File(fileName);
    if (!inputFile.canRead()) {
      throw new IOException("missing or unreadable inputPath: "
          + inputFile.getCanonicalPath());
    }

    Document loadDocument = builder.parse(inputFile);
    NodeList nodeList = loadDocument.getDocumentElement().getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      if (nodeList.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      String tagName = ((Element) nodeList.item(i)).getTagName();
      if (tagName.equalsIgnoreCase("operations")) {
        setAll(nodeList.item(i));
      }
    }
  }

  public void setAll(Node operationsNode) {
    String tmp = "";
    tmp = getNodeText((((Element) operationsNode)
        .getElementsByTagName("create").item(0)));
    if (!(tmp == null || tmp.equals("")))
      numCreate = Integer.parseInt(tmp);

    tmp = getNodeText(((Element) operationsNode).getElementsByTagName(
        "checkinterval").item(0));
    if (!(tmp == null || tmp.equals("")))
      checkInterval = Integer.parseInt(tmp);

    tmp = getNodeText(((Element) operationsNode).getElementsByTagName(
        "module_name").item(0));
    if (!(tmp == null || tmp.equals("")))
      moduleName = tmp;

    tmp = getNodeText(((Element) operationsNode).getElementsByTagName(
        "response_format").item(0));
    if (!(tmp == null || tmp.equals("")))
      responseFormat = tmp;

    loadDir = getNodeText(
        (((Element) operationsNode).getElementsByTagName("loaddir").item(0)));
    if (loadDir != null)
      loadDir = loadDir.replaceAll("QA_HOME", System.getProperty("QA_HOME"));
    //look for javascript language setting 
    tmp = getNodeText(
        (((Element) operationsNode).getElementsByTagName("language").item(0)));
    //if there is an element and it is not empty set the language
    if(! (tmp == null || tmp.equals(""))){ 
System.out.println("got the language >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + tmp); 
	setLanguage(tmp); 
    } 
    generateQuery = getNodeText((((Element) operationsNode)
        .getElementsByTagName("generatequery").item(0)));
    createModule = getNodeText((((Element) operationsNode)
        .getElementsByTagName("createmodule").item(0)));

    Element multiStmtNode = (Element) ((Element) operationsNode)
        .getElementsByTagName("multistatement").item(0);
    if (multiStmtNode != null) {
      multiStatement = true;
      setRollback(multiStmtNode.getAttribute("type").equals("rollback"));
      batchSize = Integer.parseInt(multiStmtNode.getAttribute("batchsize"));
    }

  }
  public void setLanguage(String lang){
    language = lang; 
  } 
  public void setNumCreate(int numCreate) {
    this.numCreate = numCreate;
  }

  public void setCheckInterval(int checkInterval) {
    this.checkInterval = checkInterval;
  }

  public void setLoadDir(String dir) {
    loadDir = dir;
  }

  public void setGenerateQuery(String query) {
    generateQuery = query;
  }

  public void setNumGenerated(String num) throws Exception {
    numGenerated = Integer.parseInt(num);
  }
  public String getLanguage(){ 
    return language; 
  } 
  public int getNumGenerated() {
    return numGenerated;
  }

  public String getGenerateQuery() {
    return generateQuery;
  }

  public String getLoadDir() {
    return loadDir;
  }

  public int getCheckInterval() {
    return checkInterval;
  }

  public int getNumCreate() {
    return numCreate;
  }

  public String getModuleName() {
    return moduleName;
  }

  public String getResponseFormat() {
    return responseFormat;
  }

  /**
   * Insert the method's description here.
   * 
   * @return String
   */
  public String toString() {
    String temp = super.toString() + "\n";

    temp += "numcreate = " + numCreate + "\n";
    temp += "checkInterval = " + checkInterval + "\n";
    temp += "loadDir = " + loadDir + "\n";
    temp += "createModule = " + createModule + "\n";
    temp += "autoGenerate = " + autoGenerate + "\n";
    temp += "multiStatement = " + multiStatement + "\n";
    temp += "batchSize = " + batchSize + "\n";
    temp += "rollback = " + rollback + "\n";
    temp += "generateQuery = " + generateQuery + "\n";
    temp += "numGenerated = " + numGenerated + "\n";
    temp += "response_format = " + responseFormat + "\n";
    temp += "module_name = " + moduleName + "\n";
    temp += "language = " + language + "\n"; 

    return temp;
  }
}
