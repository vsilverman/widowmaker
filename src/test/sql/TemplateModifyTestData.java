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
import test.stress.StressTestProperties;

public class TemplateModifyTestData extends TemplateLoadTestData {

  protected int maxChanges = 0;
  protected ArrayList<String> modifyList = null;

  public TemplateModifyTestData(String fileName) throws Exception {
    super(fileName);

    modifyList = new ArrayList<String>();

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
      } else if (tagName.equalsIgnoreCase("max-changes")) {
        String tmp = getNodeText((Element) nodeList.item(i));
        if (!(tmp == null || tmp.equals(""))) {
          maxChanges = Integer.parseInt(tmp);
        }
      } else if (tagName.equalsIgnoreCase("modify-fields")) {
        Element elem = (Element)nodeList.item(i);
        System.out.println("loading modify fields");
        initModifyFields(elem);
      }
    }
  }

  protected void initModifyFields(Element el) {
    if (el == null)
      return;

    NodeList modList = el.getChildNodes();
    for (int ii = 0; ii < modList.getLength(); ii++) {
      if (modList.item(ii).getNodeType() != Node.ELEMENT_NODE)
        continue;
      Element element = (Element)modList.item(ii);
      // String tagName = ((Element)modList.item(ii)).getTagName();
      String tagName = element.getTagName();
      if (tagName.equals("field")) {
        String tmp = getNodeText(element);
        if (!(tmp == null || tmp.equals(""))) {
          // System.out.println("adding to modifyList:  " + tmp);
          modifyList.add(tmp);
        }
      }
    }
  }

  public String toString() {
    StringBuilder temp = new StringBuilder(super.toString());
    temp.append("\n");
    for (String s : modifyList) {
      temp.append(s);
      temp.append(":\n");
    }
    return temp.toString();
  }
}
