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
import java.util.HashMap;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import test.utilities.BaseParseFieldManager;

public class DbTradeStoreTestData extends LoadTestData {

  public static final int DEFAULT_DUPLICATE_INTERVAL = 5000;
  public static final int DEFAULT_VERSION_INCREMENT_INTERVAL = 5000;

  ArrayList<String> lastnames;
  ArrayList<String> firstnames;
  ArrayList<String> teamnames;
  ArrayList<String> bats_choices;
  ArrayList<String> throws_choices;
  int duplicate_interval = DEFAULT_DUPLICATE_INTERVAL;
  int version_increment_interval = DEFAULT_VERSION_INCREMENT_INTERVAL;

  BaseParseFieldManager fieldManager = null;

  private String action = "";

  public DbTradeStoreTestData(String fileName) throws Exception {
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
      } else if (tagName.equalsIgnoreCase("dupe_interval")) {
        Element elem = (Element)nodeList.item(i);
        String tmp = getNodeText(elem);
        System.out.println("setting dupe_interval to " + tmp);
        int interval = Integer.parseInt(tmp);
        setDuplicateInterval(interval);
      } else if (tagName.equalsIgnoreCase("vers_incr_interval")) {
        Element elem = (Element)nodeList.item(i);
        String tmp = getNodeText(elem);
        System.out.println("setting vers_incr_interval to " + tmp);
        int interval = Integer.parseInt(tmp);
        setVersionIncrementInterval(interval);
      } else if (tagName.equalsIgnoreCase("custom-fields")) {
        Element elem = (Element)nodeList.item(i);
        System.out.println("loading custom parsing fields");
        fieldManager.initialize(elem);
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
    
  }

  public void setDuplicateInterval(int interval) {
    if (interval > 0)
      duplicate_interval = interval;
  }

  public int getDuplicateInterval() {
    return duplicate_interval;
  }

  public void setVersionIncrementInterval(int interval) {
    if (interval > 0)
      version_increment_interval = interval;
  }

  public int getVersionIncrementInterval() {
    return version_increment_interval;
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
