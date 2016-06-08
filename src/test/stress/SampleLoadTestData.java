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

/**
 * The SampleLoadTestData class is the data class for SampleLoadTester.
 *
 * @author Changfei Chen
 * @version 1.0
 * @since 2015-10-26
 */

package test.stress;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import test.utilities.BaseParseFieldManager;

public class SampleLoadTestData extends LoadTestData{

  ArrayList<String> lastnames;
  ArrayList<String> firstnames;
  BaseParseFieldManager fieldManager = null;

  public SampleLoadTestData(String fileName) throws Exception {
    super(fileName);
    lastnames = new ArrayList<String>();
    firstnames = new ArrayList<String>();

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
      if (tagName.equalsIgnoreCase("custom-fields")) { 
        System.out.println("loading custom parsing fields");
        fieldManager.initialize(nodeList.item(i));
      } 
    }
  }
}
