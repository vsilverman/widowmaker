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

import test.stress.Query;
import test.stress.TestData;
import test.stress.LoadTestData;

public class TemplateTestData extends LoadTestData {
  LoadTestData loadTestData;
  int concurrency = 1;
  int repeat = 1;
  ArrayList<Query> queries;
  HashMap<Integer, TemplateResults> results;
  HashMap<Integer, TemplateQuery> verify;
  HashMap<Integer, TemplateQuery> dump;
  int totalQueries;

  private String action = "";

  public TemplateTestData(String fileName) throws Exception {
    super(fileName);
    // loadTestData = new LoadTestData(fileName);
    queries = new ArrayList<Query>();
    results = new HashMap<Integer, TemplateResults>();
    verify = new HashMap<Integer, TemplateQuery>();
    dump = new HashMap<Integer, TemplateQuery>();

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
        setVerificationOptions(nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("testtype")) {
       	setTestTypeInfo((Element) nodeList.item(i));
      } 
    }

    totalQueries = queries.size();
  }

  public void setVerificationOptions(Node verificationNode) {

    System.out.println("TemplateTestData.setVerificationOptions");

    String tmp = "";
    tmp = getNodeText((((Element) verificationNode)
        .getElementsByTagName("concurrency").item(0)));
    if (!(tmp == null || tmp.equals("")))
      concurrency = Integer.parseInt(tmp);

    tmp = getNodeText(((Element) verificationNode).getElementsByTagName(
        "repeat").item(0));
    if (!(tmp == null || tmp.equals("")))
      repeat = Integer.parseInt(tmp);
    
/*
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
        String index = elem.getAttribute("index");
        // System.out.println("query:  index is " + index);
        if (!(tmp == null || tmp.equals(""))) {

          // System.out.println("adding query to queries:");
          // System.out.println(tmp);

          if (index == null)
            queries.add(new TemplateQuery(tmp,lan));
          else
            queries.add(new TemplateQuery(tmp, lan, Integer.parseInt(index)));
        }
      }
      if (tagName.equalsIgnoreCase("results")) {
        tmp = getNodeText((Element) queryNodes.item(i));
        String index = elem.getAttribute("index");
        System.out.println("results:  index is " + index);
        if (!(tmp == null || tmp.equals(""))) {
          if (index == null) {
            System.out.println("trouble:  query result has no index");
          } else {
            results.put(new Integer(Integer.parseInt(index)), new TemplateResults(Integer.parseInt(index), tmp));
          }
        }
      }
      if (tagName.equalsIgnoreCase("verify")) {
        tmp = getNodeText((Element) queryNodes.item(i));
        String index = elem.getAttribute("index");
        System.out.println("verify:  index is " + index);
        if (!(tmp == null || tmp.equals(""))) {
          if (index == null) {
            System.out.println("trouble:  verify query has no index");
          } else {
            verify.put(new Integer(Integer.parseInt(index)),
                      new TemplateQuery(tmp, null, Integer.parseInt(index)));
          }
        }
      }
      if (tagName.equalsIgnoreCase("dump")) {
        tmp = getNodeText((Element) queryNodes.item(i));
        String index = elem.getAttribute("index");
        System.out.println("dump:  index is " + index);
        if (!(tmp == null || tmp.equals(""))) {
          if (index == null) {
            System.out.println("trouble:  dump query has no index");
          } else {
            dump.put(new Integer(Integer.parseInt(index)),
                      new TemplateQuery(tmp, null, Integer.parseInt(index)));
          }
        }
      }
    }
*/

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

  public int getTotalQueries() {
    return totalQueries;
  }

  public String getQueryAt(int pos) {
    if (pos < queries.size())
      return queries.get(pos).query;
    else
      return null;
  }

  public String getVerifyQueryAt(int pos) {
    if (pos < verify.size())
      return verify.get(pos).query;
    else
      return null;
  }

  public TemplateQuery getQueryObjAt(int pos) {
    if (pos < queries.size())
      return (TemplateQuery)queries.get(pos);
    else
      return null;
  }

  public String getResultsAtId(int pos) {
    if (pos < results.size())
      return results.get(pos).toString();
    else
      return null;
  }

  // returns the results string for the given query index, if exists
  public String getResultsForId(int index) {
    Integer key = new Integer(index);
    TemplateResults r = (TemplateResults)results.get(key);
    if (r != null)
      return r.toString();
    else
      return null;
  }

  // returns the results string for the given query index, if exists
  public String getDumpForId(int index) {
    Integer key = new Integer(index);
    TemplateQuery r = (TemplateQuery)dump.get(key);
    if (r != null)
      return r.toString();
    else
      return null;
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
