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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import test.utilities.BaseParseFieldManager;
import test.utilities.ParseField;

public class ParserTestData extends LoadTestData {

  public static final int DEFAULT_DUPLICATE_INTERVAL = 5000;
  public static final int DEFAULT_VERSION_INCREMENT_INTERVAL = 5000;

  ArrayList<String> lastnames;
  ArrayList<String> firstnames;
  ArrayList<String> teamnames;
  ArrayList<String> bats_choices;
  ArrayList<String> throws_choices;

  BaseParseFieldManager fieldManager;

  int duplicate_interval = DEFAULT_DUPLICATE_INTERVAL;
  int version_increment_interval = DEFAULT_VERSION_INCREMENT_INTERVAL;
  int testing_suffix = 0;
  int startId = -1;   // first ID for the document identifier (-1 means not set)
  int endId = -1;   // last ID for the document identifier (-1 means not set)
  String loadTemplate;

  private String action = "";

  public ParserTestData(String fileName) throws Exception {
    super(fileName);
    lastnames = new ArrayList<String>();
    firstnames = new ArrayList<String>();
    teamnames = new ArrayList<String>();
    bats_choices = new ArrayList<String>();
    throws_choices = new ArrayList<String>();

    fieldManager = new BaseParseFieldManager();

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
      if (tagName.equalsIgnoreCase("customer_data")) {
       	setTeamData((Element) nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("custom-fields")) {
       	fieldManager.initialize((Element) nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("testing_suffix")) {
        Element elem = (Element)nodeList.item(i);
        String tmp = getNodeText(elem);
        System.out.println("setting testing_suffix to " + tmp);
        int interval = Integer.parseInt(tmp);
        setTestingSuffix(interval);
      } else if (tagName.equalsIgnoreCase("dupe_interval")) {
        Element elem = (Element)nodeList.item(i);
        String tmp = getNodeText(elem);
        System.out.println("setting dupe_interval to " + tmp);
        int interval = Integer.parseInt(tmp);
        setDuplicateInterval(interval);
      } else if (tagName.equalsIgnoreCase("start_id")) {
        Element elem = (Element)nodeList.item(i);
        String tmp = getNodeText(elem);
        System.out.println("setting start_id to " + tmp);
        int startId = Integer.parseInt(tmp);
        setStartId(startId);
      } else if (tagName.equalsIgnoreCase("end_id")) {
        Element elem = (Element)nodeList.item(i);
        String tmp = getNodeText(elem);
        System.out.println("setting end_id to " + tmp);
        int endId = Integer.parseInt(tmp);
        setEndId(endId);
      } else if (tagName.equalsIgnoreCase("load_template")) {
        Element elem = (Element)nodeList.item(i);
        String tmp = getNodeText(elem);
        System.out.println("setting load_template to " + tmp);
        setLoadTemplate(tmp);
      } else if (tagName.equalsIgnoreCase("vers_incr_interval")) {
        Element elem = (Element)nodeList.item(i);
        String tmp = getNodeText(elem);
        System.out.println("setting vers_incr_interval to " + tmp);
        int interval = Integer.parseInt(tmp);
        setVersionIncrementInterval(interval);
      } 
    }
  }

  public void setTeamData(Node teamDataNode) {
    String tmp = "";
    
    Node lastnamesNode = 
      ((Element) teamDataNode).getElementsByTagName("lastnames").item(0);
    if (lastnamesNode != null) {
      NodeList lastnamesNodes = lastnamesNode.getChildNodes();
      for (int i = 0; i < lastnamesNodes.getLength(); i++) {
        if (lastnamesNodes.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
          continue;
        }
        Element elem = ((Element) lastnamesNodes.item(i));
        String tagName = elem.getTagName();
        if (tagName.equalsIgnoreCase("lastname")) {
          tmp = getNodeText((Element) lastnamesNodes.item(i));
          if (!(tmp == null || tmp.equals(""))) {
            lastnames.add(tmp);
          }
        }
      }
    }
    
/*
    Node teamsNode = 
      ((Element) teamDataNode).getElementsByTagName("teams").item(0);
    NodeList teamNodes = teamsNode.getChildNodes();
    for (int i = 0; i < teamNodes.getLength(); i++) {
      if (teamNodes.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      Element elem = ((Element) teamNodes.item(i));
      String tagName = elem.getTagName();
      if (tagName.equalsIgnoreCase("team")) {
        tmp = getNodeText((Element) teamNodes.item(i));
        if (!(tmp == null || tmp.equals(""))) {
          teamnames.add(tmp);
        }
      }
    }
*/
    
    Node firstnamesNode = 
      ((Element) teamDataNode).getElementsByTagName("firstnames").item(0);
    if (firstnamesNode != null) {
      NodeList firstnamesNodes = firstnamesNode.getChildNodes();
      for (int i = 0; i < firstnamesNodes.getLength(); i++) {
        if (firstnamesNodes.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
          continue;
        }
        Element elem = ((Element) firstnamesNodes.item(i));
        String tagName = elem.getTagName();
        if (tagName.equalsIgnoreCase("firstname")) {
          tmp = getNodeText((Element) firstnamesNodes.item(i));
          if (!(tmp == null || tmp.equals(""))) {
            firstnames.add(tmp);
          }
        }
      }
    }
    
/*
    Node batsNode = 
      ((Element) teamDataNode).getElementsByTagName("bats_choices").item(0);
    NodeList batsNodes = batsNode.getChildNodes();
    for (int i = 0; i < batsNodes.getLength(); i++) {
      if (batsNodes.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      Element elem = ((Element) batsNodes.item(i));
      String tagName = elem.getTagName();
      if (tagName.equalsIgnoreCase("bats_choice")) {
        tmp = getNodeText((Element) batsNodes.item(i));
        if (!(tmp == null || tmp.equals(""))) {
          bats_choices.add(tmp);
        }
      }
    }
*/
    
/*
    Node throwsNode = 
      ((Element) teamDataNode).getElementsByTagName("throws_choices").item(0);
    NodeList throwNodes = throwsNode.getChildNodes();
    for (int i = 0; i < throwNodes.getLength(); i++) {
      if (throwNodes.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      Element elem = ((Element) throwNodes.item(i));
      String tagName = elem.getTagName();
      if (tagName.equalsIgnoreCase("throws_choice")) {
        tmp = getNodeText((Element) throwNodes.item(i));
        if (!(tmp == null || tmp.equals(""))) {
          throws_choices.add(tmp);
        }
      }
    }
*/
  }

  public void setTestingSuffix(int suffix) {
    testing_suffix = suffix;
  }

  public int getTestingSuffix() {
    return testing_suffix;
  }

  public void setLoadTemplate(String template) {

    loadTemplate = template.replaceAll("QA_HOME", System.getProperty("QA_HOME"));

    System.out.println("loadTemplate is " + loadTemplate);
  }

  public String getLoadTemplate() {
    return loadTemplate;
  }

  public void setDuplicateInterval(int interval) {
    if (interval > 0)
      duplicate_interval = interval;
  }

  public int getDuplicateInterval() {
    return duplicate_interval;
  }

  public void setStartId(int id) {
    if (id > 0)
      startId = id;
  }

  public int getStartId() {
    return startId;
  }

  public void setEndId(int id) {
    if (id > 0)
      endId = id;
  }

  public int getEndId() {
    return endId;
  }

  public void setVersionIncrementInterval(int interval) {
    if (interval > 0)
      version_increment_interval = interval;
  }

  public int getVersionIncrementInterval() {
    return version_increment_interval;
  }

  public ParseField getCustomField(String token) {
    ParseField field = fieldManager.getParseField(token);

    return field;
  }

  public String toString() {
    StringBuilder temp = new StringBuilder(super.toString());
    temp.append("\n");
    temp.append("duplicate_interval: ");
    temp.append(duplicate_interval);
    temp.append("\n");
    temp.append("vers_incr_interval: ");
    temp.append(version_increment_interval);
    temp.append("\n");

    return temp.toString();
  }
}
