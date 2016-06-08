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

import java.net.URL;
import java.net.MalformedURLException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class HttpTestData extends TestData {
  LoadTestData loadTestData;
  int concurrency = 1;
  int repeat = 1;
  String method;
  URL url;


  ArrayList<Query> queries;

  HashMap<Integer, InferenceResults> results;
  HashMap<Integer, InferenceQuery> verify;
  HashMap<Integer, InferenceQuery> dump;

  int totalQueries;

  private String action = "";

  public HttpTestData(String fileName) throws Exception {
    super(fileName);
    loadTestData = new LoadTestData(fileName);

/*
    queries = new ArrayList<Query>();
    results = new HashMap<Integer, InferenceResults>();
    verify = new HashMap<Integer, InferenceQuery>();
    dump = new HashMap<Integer, InferenceQuery>();
*/

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
      // left this just to have something to watch for an entire block
      if (tagName.equalsIgnoreCase("http_info")) {
        setHttpInfo(nodeList.item(i));
      } 
    }

  }

  public void setHttpInfo(Node httpNode) {
    String tmp = "";
    tmp = getNodeText((((Element) httpNode)
        .getElementsByTagName("method").item(0)));
    if (!(tmp == null || tmp.equals("")))
      method = tmp;

    tmp = getNodeText(((Element) httpNode).getElementsByTagName(
        "url").item(0));
    if (!(tmp == null || tmp.equals(""))) {
      try {
      url = new URL(tmp);
      } catch (MalformedURLException e) {
        e.printStackTrace();
      }
    }
    
  }
  
  public String toString() {
    StringBuilder temp = new StringBuilder(super.toString());
    temp.append("\nmethod = "); 
    temp.append(method);
    temp.append("\nurl = ");
    temp.append(url.toString());
    temp.append("\n");
    return temp.toString();
  }
}
