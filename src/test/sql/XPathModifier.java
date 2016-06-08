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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import org.jdom2.Document;
import org.jdom2.Content;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.jdom2.filter.Filters;

import test.utilities.TemplateParser;
import test.utilities.ParseFieldManager;
import test.utilities.ParseField;
import test.utilities.BaseParseFieldManager;


/**
 * Class to wrap the updating of individual elements for a document
 * based in part on a document template combined with parse field
 * handling.
 */
public class XPathModifier {

  String filename = null;
  ParseFieldManager fieldManager = null;
  TemplateParser templateParser = null;
  String prefix = null;
  String uri = null;
  ArrayList<Namespace> namespaces = null;

  protected boolean debugFlag = false;

  public XPathModifier() {
    namespaces = new ArrayList<Namespace>();
  }

  public XPathModifier(String filename, String elementToFind) {
    this.filename = filename;
    namespaces = new ArrayList<Namespace>();
  }

  public XPathModifier(String filename, String prefix, String uri) {

    this.filename = filename;
    this.prefix = prefix;
    this.uri = uri;
    namespaces = new ArrayList<Namespace>();
  }

  public void addNamespace(String prefix, String uri) {
    if ((prefix == null) || (uri == null))
      return;

    if (debugFlag)
      System.out.println("adding namespace:  " + prefix + ", " + uri);

    Namespace namespace = Namespace.getNamespace(prefix, uri);
    if (namespace != null) {
      if (namespaces == null) {
        namespaces = new ArrayList<Namespace>();
      }
      namespaces.add(namespace);
    } else {
      System.out.println("namespace came back null");
    }
  }

  public void setFilename(String filename) {
    this.filename = filename;

    File f = new File(filename);
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(f);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  public void setFieldManager(ParseFieldManager fieldManager) {
    this.fieldManager = fieldManager;
  }

  public void setTemplateParser(TemplateParser templateParser) {
    this.templateParser = templateParser;
  }

  public String getNewElementValue(String elementToFind) {

    if (elementToFind == null)
      return null;

    if (fieldManager == null) {
      System.out.println("getNewElementValue:  ParseFieldManager is not set");
      return null;
    }

    String newValue = null;
    String currValue = getElementValue(filename, elementToFind);
    if (templateParser == null) {
      ParseField field = fieldManager.getParseField(currValue);
      if (field == null)
        return currValue;
      newValue = field.generateData(currValue);
    } else {
      newValue = templateParser.parseField(currValue);
    }
    return newValue;
  }

  String getElementValue(String filename, String elementToFind) {

    if ((filename == null) || (elementToFind == null))
      return null;

    File f = new File(filename);
    FileInputStream fis = null;
    if (!f.exists()) {
      System.out.println("file does not exist:  " + filename);
      return null;
    }

    try {
      fis = new FileInputStream(f);
    } catch (FileNotFoundException e) {
      System.out.println("file does not exist:  " + filename);
      return null;
    }

    return getElementValue(fis, elementToFind);
  }

  String getElementValue(InputStream is, String elementToFind) {

    SAXBuilder builder = new SAXBuilder();
    Document doc = null;

    if (debugFlag)
      System.out.println("getElementValue(is, element)");

    if ((is == null) || (elementToFind == null)) {
      return null;
    }

    try {
      doc = builder.build(is);
    } catch (JDOMException e) {
      e.printStackTrace();
      return null;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }

    return getElementValue(doc, elementToFind);
  }

  String getElementValue(Document doc, String elementToFind) {

    return getElementValueNew(doc, elementToFind);
  }

  String getElementValueNew(Document doc, String elementToFind) {

    String val = null;

    if ((doc == null) || (elementToFind == null))
      return null;

    if (debugFlag)
      System.out.println("getElementValueNew:  looking for " + elementToFind);

    XPathFactory xpf = XPathFactory.instance();
    if (debugFlag)
      System.out.println("number of namespaces:  " + namespaces.size());

    XPathExpression<Element> xpath = xpf.compile(elementToFind,
                                      Filters.element(), null,
                                      namespaces);

    Element el = xpath.evaluateFirst(doc);

    return el.getText();
  }

  protected void debugLog(String str) {
    if (debugFlag)
      System.out.println(str);
  }

  void test() {

    File f = null;
    FileInputStream fis = null;

    try {
    f = new File(filename);
    fis = new FileInputStream(f);
    SAXBuilder builder = new SAXBuilder();
    Document doc = null;
    try {
      doc = builder.build(fis);
    } catch (JDOMException e) {
      e.printStackTrace();
      return;
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

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

  void test2() {

    File f = null;
    FileInputStream fis = null;

    try {
    f = new File(filename);
    fis = new FileInputStream(f);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    String txInstant = null;
    txInstant = getElementValue(fis, "/dbts:PmryObj/dbts:HseKpg/dbts:TxInstant");
    if (txInstant != null) {
      System.out.println("would be setting TxInstant to something different:  " + txInstant);
      // record.setLastDate(txInstant);
    } else {
      System.out.println("TxInstant came out null");
    }
    System.out.println("existing value:  " + txInstant);

    txInstant = getNewElementValue("/dbts:PmryObj/dbts:HseKpg/dbts:TxInstant");
    System.out.println("new value:  " + txInstant);
  }


  public static final void main(String[] args) {

    XPathModifier tester = null;

    String filename = args[0];

    tester = new XPathModifier(filename, "dbts", "/dbtradestore/core/");
    tester.addNamespace("dbts", "/dbtradestore/core/");

    tester.test();

    System.out.println("trying again");

    tester = new XPathModifier();
    tester.addNamespace("dbts", "/dbtradestore/core/");
    tester.setFilename(filename);
    ParseFieldManager fieldManager = new BaseParseFieldManager();
    tester.setFieldManager(fieldManager);
    tester.test2();

    System.exit(0);

  }

}
