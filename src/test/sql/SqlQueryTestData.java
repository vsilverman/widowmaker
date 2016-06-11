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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import test.utilities.BaseParseFieldManager;
import test.stress.Query;
import test.stress.QueryTestData;
import test.stress.StressTestProperties;

public class SqlQueryTestData extends QueryTestData {
  protected boolean doVerify = true;

  protected String host = null;
  protected int port = 0;
  protected String user = null;
  protected String password = null;

  protected String updateTemplate = null;

  protected BaseParseFieldManager fieldManager = null;

  private String action = "";

  public SqlQueryTestData(String fileName) throws Exception {
    super(fileName);

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    File inputFile = new File(fileName);
    if (!inputFile.canRead()) {
      throw new IOException("missing or unreadable inputPath: "
          + inputFile.getCanonicalPath());
    }

    fieldManager = new BaseParseFieldManager();

    // load the defaults we're using for the whole suite
    initSharedFields(fieldManager);

    Document loadDocument = builder.parse(inputFile);
    NodeList nodeList = loadDocument.getDocumentElement().getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      if (nodeList.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      String tagName = ((Element) nodeList.item(i)).getTagName();
      if (tagName.equalsIgnoreCase("do_verify")) {
        String tmp = getNodeText((Element) nodeList.item(i));
        if (!(tmp == null || tmp.equals(""))) {
          if (tmp.equalsIgnoreCase("false"))
            doVerify = false;
        }
      } else if (tagName.equalsIgnoreCase("update_template")) {
        String tmp = getNodeText((Element) nodeList.item(i));
        if (!(tmp == null || tmp.equals(""))) {
          updateTemplate = expandQaPath(tmp);
        }
      } else if (tagName.equalsIgnoreCase("custom-fields")) {
        Element elem = (Element)nodeList.item(i);
        System.out.println("loading custom parsing fields");
        fieldManager.initialize(elem);
      }
    }
  }

  protected void initSharedFields(BaseParseFieldManager fieldManager)
      throws Exception {
    StressTestProperties props = StressTestProperties.getStressTestProperties();

    if (props == null) {
      System.out.println("no StressTestProperties present");
      return;
    }
    String defaultFields = props.getPropertyAsPath("test.utilities.ParseFieldManager.defaults");
    if (defaultFields == null) {
      System.out.println("no defaults specified in properties");
      return;
    }

    File f = new File(defaultFields);
    if (!f.exists()) {
      System.out.println("ParseFieldManager defaults file does not exist:  " + defaultFields);
      return;
    }

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document defaultsDocument = builder.parse(defaultFields);
    NodeList nodeList = defaultsDocument.getDocumentElement().getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      if (nodeList.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      String tagName = ((Element)nodeList.item(i)).getTagName();
      if (tagName.equalsIgnoreCase("custom-fields")) {
        Element elem = (Element)nodeList.item(i);
        // System.out.println("loading default custom parsing fields");
        fieldManager.initialize(elem);
      }
    }

    // fieldManager.dumpFieldList(System.out);

  }

  public String toString() {
    StringBuilder temp = new StringBuilder(super.toString());
    temp.append("\nconcurrency = "); 
    temp.append(concurrency);
    temp.append("\nrepeat = ");
    temp.append(repeat);
    temp.append("\n");
    for (Query q : queries) {
      temp.append(q.language);
      temp.append(":\n");
      temp.append(q.query); 
      temp.append("\n");
    }
    return temp.toString();
  }
}
