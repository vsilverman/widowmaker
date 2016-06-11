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

public class InferenceLoadTestData extends InferenceTestData {
  ArrayList<String> lastnames;
  ArrayList<String> firstnames;
  ArrayList<String> teamnames;
  ArrayList<String> bats_choices;
  ArrayList<String> throws_choices;
  protected boolean doVerify = true;

  private String action = "";

  public InferenceLoadTestData(String fileName) throws Exception {
    super(fileName);
    lastnames = new ArrayList<String>();
    firstnames = new ArrayList<String>();
    teamnames = new ArrayList<String>();
    bats_choices = new ArrayList<String>();
    throws_choices = new ArrayList<String>();

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
      if (tagName.equalsIgnoreCase("team_data")) {
       	setTeamData((Element) nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("do_verify")) {
        String tmp = getNodeText((Element) nodeList.item(i));
        if (!(tmp == null || tmp.equals(""))) {
          if (tmp.equalsIgnoreCase("false"))
            doVerify = false;
        }
      }
    }
  }

  public void setTeamData(Node teamDataNode) {
    String tmp = "";
    
    Node lastnamesNode = 
      ((Element) teamDataNode).getElementsByTagName("lastnames").item(0);
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
    
    Node firstnamesNode = 
      ((Element) teamDataNode).getElementsByTagName("firstnames").item(0);
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
