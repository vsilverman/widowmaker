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


import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.util.Random;
import java.util.ArrayList;
import java.util.HashMap;

import test.utilities.ParseField;
import test.utilities.ParseFieldManager;


public class ParseConsistentCustomerGenerator
    implements ParseField {

    // a handle to the parent resources
    ParseFieldManager parseFieldManager = null;

    // the token we get triggered by
    String  token;
    int   minValue;
    int   maxValue;
    boolean useCurrent = false;
    ArrayList<String> lastNames = null;
    ArrayList<String> firstNames = null;

  /**
   * we need a no-arg default constructor in order to be loaded by the
   * class loader
   */
  public ParseConsistentCustomerGenerator() {
    lastNames = new ArrayList<String>();
    firstNames = new ArrayList<String>();
  }

  public String getToken() {
    return token;
  }

  public void initialize(ParseFieldManager manager, Node t) {
    String tmp;

    parseFieldManager = manager;

    if ((t == null) || (t.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE))
      return;
    NodeList children = t.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      if (children.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      String tagName = ((Element) children.item(i)).getTagName();
      if (tagName.equals("token")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals("")))
          token = tmp;
      }
      if (tagName.equals("min")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals("")))
          minValue = Integer.parseInt(tmp);
      }
      else if (tagName.equals("max")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals("")))
          maxValue = Integer.parseInt(tmp);
      }

      // custom values of your own choosing go here
      else if (tagName.equals("use_current")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals("")))
          useCurrent = Boolean.parseBoolean(tmp);
      }
      if (tagName.equalsIgnoreCase("name_data")) {
       	setNameData((Element) children.item(i));
      } 
    }
  }

  protected void setNameData(Node t) {
    String tmp = "";
    
    Node lastnamesNode = 
      ((Element) t).getElementsByTagName("lastnames").item(0);
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
          lastNames.add(tmp);
        }
      }
    }
    
    Node firstnamesNode = 
      ((Element) t).getElementsByTagName("firstnames").item(0);
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
          firstNames.add(tmp);
        }
      }
    }
  }







  protected String getNodeText(Node t) {
    if ((t == null) || (t.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE))
      return null;
    NodeList children = t.getChildNodes();
    String text = "";
    for (int c = 0; c < children.getLength(); c++) {
      Node child = children.item(c);
      if ((child.getNodeType() == org.w3c.dom.Node.TEXT_NODE)
          || (child.getNodeType() == org.w3c.dom.Node.CDATA_SECTION_NODE)) {
        if (!isWhitespaceNode(child))
          text += child.getNodeValue();
      }
    }
    return text;
  }

  private boolean isWhitespaceNode(Node t) {
    if (t.getNodeType() == org.w3c.dom.Node.TEXT_NODE) {
      String val = t.getNodeValue();
      return val.trim().length() == 0;
    }
    else
      return false;
  }

  public String generateData(String token) {

    String str = null;

    int pos;
    int count;

    Random r = parseFieldManager.getRandomNumberGenerator();

    if (lastNames.size() == 0 || firstNames.size() == 0) {
      str = "first last";
    } else {
      count = firstNames.size();
      pos = -1;
      while (pos < 0)
        pos = r.nextInt() % count;

      str = (String)firstNames.get(pos);
      str += " ";

      count = lastNames.size();
      pos = -1;
      while (pos < 0)
        pos = r.nextInt() % count;
      str += (String)lastNames.get(pos);
    }

    return str;
  }

  class NameGenData {
    int custId = 0;
    String firstName = null;
    String lastName = null;

    NameGenData() {
    }

  }

  public static final String CUSTOM_NAME_FIELD = "ParseConsistentCustomerNameGenerator";

  public static final String CUSTOM_NAME_CUST_ID_TOKEN = "__CUST_ID__";
  public static final String CUSTOM_NAME_FIRST_NAME_TOKEN = "__CUST_FIRST_NAME__";
  public static final String CUSTOM_NAME_LAST_NAME_TOKEN = "__CUST_LAST_NAME__";

  public String generateData(String token, HashMap contextData) {
    String rval = null;
    NameGenData data = null;

    data = (NameGenData)contextData.get(CUSTOM_NAME_FIELD);
    if (data == null) {
      data = new NameGenData();
      contextData.put(CUSTOM_NAME_FIELD, data);
    }

    Random r = parseFieldManager.getRandomNumberGenerator();

    if (token.equals(CUSTOM_NAME_CUST_ID_TOKEN)) {
      if (data.custId == 0) {
        data.custId = r.nextInt();
        rval = Integer.toString(data.custId);
      }
    }
    if (token.equals(CUSTOM_NAME_FIRST_NAME_TOKEN)) {
      if (data.firstName == null) {
        data.firstName = firstNames.get(r.nextInt());
        rval = data.firstName;
      }
    }
    if (token.equals(CUSTOM_NAME_LAST_NAME_TOKEN)) {
      if (data.lastName == null) {
        data.lastName = lastNames.get(r.nextInt());
        rval = data.lastName;
      }
    }
    return rval;

  }

}

