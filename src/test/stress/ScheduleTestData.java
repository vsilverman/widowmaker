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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

public class ScheduleTestData
        extends TestData {

  protected String cronString = null;
  protected int interval = 0;

  public ScheduleTestData() {
    super();
  }

  public ScheduleTestData(String configFile)
            throws Exception {
    super(configFile);

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
      if (tagName.equalsIgnoreCase("trigger")) {
        setTriggerData(testList.item(ii));
      }
      if (tagName.equalsIgnoreCase("crontab")) {
        tmp = getNodeText((Element)testList.item(ii));
        if (!(tmp ==  null || tmp.equals("")))
          cronString = tmp;
      }
    }

  }

  void setTriggerData(Node triggerNode) {

    String tmp;

    NodeList triggerNodes = ((Element)triggerNode).getChildNodes();
    for (int ii = 0; ii < triggerNodes.getLength(); ii++) {
      if (triggerNodes.item(ii).getNodeType() != Node.ELEMENT_NODE)
        continue;
      Element elem = (Element)triggerNodes.item(ii);
      String tagName = elem.getTagName();
      if (tagName.equalsIgnoreCase("interval")) {
        tmp = getNodeText(elem);
        if (!(tmp == null || tmp.equals(""))) {
          interval = Integer.parseInt(tmp);
          // System.out.println("interval set to " + interval);
        }
      }
      if (tagName.equalsIgnoreCase("crontab")) {
        tmp = getNodeText(elem);
        if (!(tmp == null || tmp.equals(""))) {
          cronString = tmp;
          // System.out.println("cronString set to " + cronString);
        }
      }
    }

  }

  public String getCronString() {
    return cronString;
  }

  public int getInterval() {
    return interval;
  }
}


