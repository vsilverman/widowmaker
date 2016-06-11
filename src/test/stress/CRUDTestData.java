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
 * CRUDTestData class holds all information about what actions are to be performed by XccStressTester
 *
 *************************************************************************************************/
package test.stress;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Insert the type's description here.
 */
public class CRUDTestData extends LoadTestData {

  private String action = "";

  /**
   * Insert the method's description here.
   * 
   * @param fileName
   *          String
   */
  public CRUDTestData(String fileName) throws Exception {
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
      if (tagName.equalsIgnoreCase("testtype")) {
        setTestTypeInfo((Element) nodeList.item(i));
        break;
      }
    }
  }

  public void setTestTypeInfo(Element testTypeElement) {
    if (testTypeElement != null) {
      action = testTypeElement.getAttribute("action");

    }
  }

  public String getAction() {
    return action;
  }

  /**
   * Insert the method's description here.
   * 
   * @return String
   */
  public String toString() {
    String temp = super.toString() + "\n";

    temp += "action = " + action + "\n";

    return temp;
  }
}
