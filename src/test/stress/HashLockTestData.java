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
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class HashLockTestData extends TestData {
  private int numURIs = 0;
  private int numThreads = 0;

  public HashLockTestData(String fileName) throws Exception {
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
      Node node = nodeList.item(i);
      if (node.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      String tagName = ((Element) node).getTagName();
      if (tagName.equalsIgnoreCase("operations")) {

        String tmp = "";
        tmp = getNodeText((((Element) node).getElementsByTagName("numuris")
            .item(0)));
        if (!(tmp == null || tmp.equals("")))
          numURIs = Integer.parseInt(tmp);

        tmp = getNodeText((((Element) node).getElementsByTagName("numthreads")
            .item(0)));
        if (!(tmp == null || tmp.equals("")))
          numThreads = Integer.parseInt(tmp);
      }
    }
  }

  public int getNumURIs() {
    return numURIs;
  }

  public int getNumThreads() {
    return numThreads;
  }

  public String toString() {
    String temp = super.toString() + "\n";

    temp += "numURIs = " + numURIs + "\n";
    temp += "numThreads = " + numThreads + "\n";

    return temp;
  }
}
