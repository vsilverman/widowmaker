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
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Attr;

public class QueryTestData extends TestData {
  protected LoadTestData loadTestData;
  protected int concurrency = 1;
  protected int repeat = 1;
  protected ArrayList<Query> queries;
  protected ArrayList<String> results;

  private String action = "";

  public QueryTestData(String fileName) throws Exception {
    super(fileName);
    loadTestData = new LoadTestData(fileName);
    queries = new ArrayList<Query>();
    results = new ArrayList<String>();

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
      if (tagName.equalsIgnoreCase("verification")) {
        setAll(nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("testtype")) {
       	setTestTypeInfo((Element) nodeList.item(i));
      } 
    }
  }

  public void setAll(Node verificationNode) {
    String tmp = "";
    tmp = getNodeText((((Element) verificationNode)
        .getElementsByTagName("concurrency").item(0)));
    if (!(tmp == null || tmp.equals("")))
      concurrency = Integer.parseInt(tmp);

    tmp = getNodeText(((Element) verificationNode).getElementsByTagName(
        "repeat").item(0));
    if (!(tmp == null || tmp.equals("")))
      repeat = Integer.parseInt(tmp);
    
    Node queriesNode = 
      ((Element) verificationNode).getElementsByTagName("queries").item(0);
    NodeList queryNodes = queriesNode.getChildNodes();
    for (int i = 0; i < queryNodes.getLength(); i++) {
      if (queryNodes.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      Element elem = ((Element) queryNodes.item(i));
      String tagName = elem.getTagName();
      if (tagName.equalsIgnoreCase("query")) {
        tmp = getNodeText((Element) queryNodes.item(i));
        String lan = elem.getAttribute("language");
        if (!(tmp == null || tmp.equals(""))) {
          Query query = new Query(tmp, lan);
          queries.add(query);
          String attr = elem.getAttribute("result");
          if (!(attr == null || attr.equals(""))) {
              results.add(attr);
          } else {
              results.add("");
          }
          tmp = elem.getAttribute("phase");
          if (!(tmp == null || tmp.equals(""))) {
            query.setPhase(tmp);
          }

          NamedNodeMap attributes = elem.getAttributes();
          // get the number of nodes in this map
          int numAttrs = attributes.getLength();
          for (int ii = 0; ii < numAttrs; ii++) {
            Attr qattr = (Attr) attributes.item(ii);
            String attrName = qattr.getNodeName();
            String attrValue = qattr.getNodeValue();
            // debugLog(nodeIndent + "Found attribute: " + attrName + " with value: " + attrValue);
            query.addAttribute(attrName, attrValue);
          }

        }
      }
    }
  }
  
  public void updateCollection(String collection) {
    for (Query query : queries) {
      query.query = query.query.replaceAll("_COLLECTION_", collection);
    }
  }

  public void updateQAHome() {
    for (Query query : queries) {
      query.query = query.query.replaceAll("_QA_HOME_", 
          System.getProperty("QA_HOME"));
    }
  }

  public void setTestTypeInfo(Element testTypeElement) {
    if (testTypeElement != null) {
      setTestType(getNodeText(testTypeElement));
      action = testTypeElement.getAttribute("action");
    }
  }

  /**
   * I have no idea why this object is buried instead of subclassed
   * In order for other packages to extend the class and still have access
   * to this embedded test data object, I have to expose it somehow.
   */
  public LoadTestData getLoadTestData() {
    return loadTestData;
  }

  public ArrayList<Query> getQueries() {
    return queries;
  }

  public String getAction() {
    return action;
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
