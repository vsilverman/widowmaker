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

// package test.sql;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.TimeZone;
import java.util.HashMap;
import java.util.Random;
import java.util.StringTokenizer;

import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.namespace.NamespaceContext;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;
import org.w3c.dom.Attr;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.SAXException;



public class XPathTest {


  String filename = null;
  String prefix = null;
  String uri = null;

  protected boolean debugFlag = true;

  public XPathTest(String filename, String prefix, String uri) {

    this.filename = filename;
    this.prefix = prefix;
    this.uri = uri;
  }


  String getElementValueNew(Document doc, String elementToFind) {

    String val = null;

    if ((doc == null) || (elementToFind == null))
      return null;

    System.out.println("getElementValueNew:  looking for " + elementToFind);

    try {
    
    XPath xpath = XPathFactory.newInstance().newXPath();
    myNamespaceContext myContext = new myNamespaceContext(prefix, uri);
    xpath.setNamespaceContext(myContext);

    XPathExpression expr = xpath.compile(elementToFind);
    String Sresult = expr.evaluate(doc.getDocumentElement());
    System.out.println("expr evaluation as string:  " + Sresult);
    Object result = expr.evaluate(doc.getDocumentElement(), XPathConstants.NODESET);
    NodeList nodes = (NodeList) result;
    for (int ii = 0; ii < nodes.getLength(); ii++) {
      Node currentItem = nodes.item(ii);
      System.out.println("pos " + ii + " found node -> " + currentItem.getLocalName()
                          + " (namespace: " + currentItem.getNamespaceURI() + ")");
    }
    Element el = (Element)nodes.item(0);
    
      String tagName = el.getTagName();
        val = getNodeText(el);


    } catch (XPathExpressionException e) {
      e.printStackTrace();
    }

    return val;
  }

  private boolean isWhitespaceNode(Node t) {
    if (t.getNodeType() == org.w3c.dom.Node.TEXT_NODE) {
      String val = t.getNodeValue();
      return val.trim().length() == 0;
    }
    else
      return false;
  }

  protected String getNodeText(Node t) {
    if ((t == null) || (t.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE))
      return null;
    NodeList children = t.getChildNodes();
    String text = "";
    for (int c = 0; c < children.getLength(); c++) {
      Node child = children.item(c);
      if ((child.getNodeType() == org.w3c.dom.Node.TEXT_NODE)
          || (child.getNodeType() == org.w3c.dom.Node.CDATA_SECTION_NODE))
      {
        if (!isWhitespaceNode(child))
          text += child.getNodeValue();
      }
    }
    return text;
  }


  protected void debugLog(String str) {
    if (debugFlag)
      System.out.println(str);
  }

  private Document
  parseDoc(InputStream is) {
    File file = null;

    DocumentBuilderFactory dbFactory;
    DocumentBuilder dBuilder;
    Document doc = null;
    try
    {
      dbFactory = DocumentBuilderFactory.newInstance();
      dbFactory.setNamespaceAware(true);
      dBuilder = dbFactory.newDocumentBuilder();
      doc = dBuilder.parse(is);
      doc.getDocumentElement().normalize();
    }
    catch (ParserConfigurationException e) {
      debugLog("making document");
      e.printStackTrace();
    }
    catch (SAXException e) {
      debugLog("Exception parsing response");
      e.printStackTrace();
    }
    catch (IOException e) {
      debugLog("Exception parsing response");
      e.printStackTrace();
    }
    finally {
      try {
        is.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return doc;
  }

  class myNamespaceContext
    implements NamespaceContext {
    String prefix = null;
    String namespaceUri = null;

    myNamespaceContext(String prefix, String uri) {
      this.prefix = prefix;
      this.namespaceUri = uri;
    }

    public String getNamespaceURI(String prefix) {
      return prefix.equals(this.prefix) ? namespaceUri : null;
    }

    public Iterator<?> getPrefixes(String val) {

      if (val == null)
        throw new IllegalArgumentException("uri may not be null");
      if (val.equals(namespaceUri)) {
        List<String> prefixes = Arrays.asList(prefix);
        return prefixes.iterator();
      }

      return null;
    }

    public String getPrefix(String uri) {

      if (uri == null)
        throw new IllegalArgumentException("uri may not be null");
      if (uri.equals(namespaceUri))
        return prefix;

      return null;
    }
  }

  void test() {

    File f = null;
    FileInputStream fis = null;

    try {
    f = new File(filename);
    fis = new FileInputStream(f);
    Document doc = parseDoc(fis);

    String txInstant = getElementValueNew(doc, "/dbts:PmryObj/dbts:HseKpg/dbts:TxInstant");
    if (txInstant != null) {
      System.out.println("would be setting TxInstant to something different:  " + txInstant);
      // record.setLastDate(txInstant);
    } else {
      System.out.println("TxInstant came out null");
    }
    } catch (FileNotFoundException e) {
      System.err.println("XML file not found:  " + filename);
    }
  }


  public static final void main(String[] args) {

    XPathTest tester = null;

    String filename = "Gout.xml";

    tester = new XPathTest(filename, "dbts", "/dbtradestore/core/");

    tester.test();

    filename = "Goutbad.xml";

    tester = new XPathTest(filename, "dbts", "http://dbtradestore/core/");

    tester.test();

    System.exit(0);

  }

}
