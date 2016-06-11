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
import java.util.HashMap;
import java.util.Iterator;

import test.stress.ScheduleTestData;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.w3c.dom.Attr;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import test.utilities.DomUtils;

public class SqlPropModTestData
        extends ScheduleTestData {

    class PropertyModifier {
      protected String propName;
      protected int interval;
      protected int increment;

      PropertyModifier() {}

      public void setPropertyName(String propName) {
        this.propName = propName;
      }

      public String getPropertyName() {
        return propName;
      }

      public void setInterval(int interval) {
        this.interval = interval;
      }

      public int getInterval() {
        return interval;
      }
      
      public void setIncrement(int increment) {
        this.increment = increment;
      }

      public int getIncrement() {
        return increment;
      }
    }

    HashMap<String, PropertyModifier> modifiers = null;

  public SqlPropModTestData() {
    modifiers = new HashMap<String, PropertyModifier>();
  }

  public SqlPropModTestData(String configFile)
          throws Exception {
    super(configFile);
    modifiers = new HashMap<String, PropertyModifier>();
    initialize(configFile);
  }

  public void initialize(String configFile)
          throws Exception {

    if (configFile == null)
      return;

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    File inputFile = new File(configFile);
    if (!inputFile.canRead()) {
      throw new IOException("missing or unreadable inputPath: "
        + inputFile.getCanonicalPath());
    }

    String tmp = "";

    Document testDocument = builder.parse(inputFile);
    NodeList testList = testDocument.getDocumentElement().getChildNodes();
    for (int ii = 0; ii < testList.getLength(); ii++) {
      if (testList.item(ii).getNodeType() != Node.ELEMENT_NODE) {
        continue;
      }
      String tagName = ((Element)testList.item(ii)).getTagName();
/*
      if (tagName.equalsIgnoreCase("trigger")) {
        setTriggerData(testList.item(ii));
      }
      if (tagName.equalsIgnoreCase("crontab")) {
        tmp = DomUtils.getNodeText((Element)testList.item(ii));
        if (!(tmp ==  null || tmp.equals("")))
          cronString = tmp;
      }
*/
    }

    Node modNode = 
      ((Element) testList).getElementsByTagName("modifications").item(0);

    if (modNode != null)
      loadModifiers(modNode);

  }

  protected void loadModifiers(Node modNode)
                  throws Exception {

    NodeList modifications = modNode.getChildNodes();
    for (int i = 0; i < modifications.getLength(); i++) {
      if (modifications.item(i).getNodeType() != Node.ELEMENT_NODE) {
        continue;
      }
      Element elem = ((Element) modifications.item(i));
      String tagName = elem.getTagName();
      if (tagName.equalsIgnoreCase("modifier")) {
        PropertyModifier modifier = null;
        NodeList els = elem.getChildNodes();
        for (int jj = 0; jj < els.getLength(); jj++) {
          if (els.item(jj).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE)
            continue;
          Element modParam = (Element)els.item(jj);
          String elTag = modParam.getTagName();
          if (elTag.equalsIgnoreCase("property")) {
            String tmp = DomUtils.getNodeText(modParam);
            // System.out.println("processing property " + tmp);
            if (tmp != null && !tmp.equals("")) {
              if (modifier == null)
                modifier = new PropertyModifier();
              modifier.setPropertyName(tmp);
            }
          }
          if (elTag.equalsIgnoreCase("interval-secs")) {
            String tmp = DomUtils.getNodeText(modParam);
            // System.out.println("processing interval-secs " + tmp);
            if (tmp != null && !tmp.equals("")) {
              if (modifier == null)
                modifier = new PropertyModifier();
              modifier.setInterval(Integer.parseInt(tmp));
            }
          }
          if (elTag.equalsIgnoreCase("increment")) {
            String tmp = DomUtils.getNodeText(modParam);
            // System.out.println("processing increment " + tmp);
            if (tmp != null && !tmp.equals("")) {
              if (modifier == null)
                modifier = new PropertyModifier();
              modifier.setIncrement(Integer.parseInt(tmp));
            }
          }

        }

        if (modifier != null)
          modifiers.put(modifier.propName, modifier);
      }
    }
  }

  public Iterator propertyNames() {

    Iterator iterator = modifiers.keySet().iterator();

    return iterator;

  }

  public int getInterval(String propName) {

    PropertyModifier modifier = (PropertyModifier)modifiers.get(propName);

    return modifier.getInterval();
  }

  public int getIncrement(String propName) {
    PropertyModifier modifier = (PropertyModifier)modifiers.get(propName);

    return modifier.getIncrement();
  }

}

