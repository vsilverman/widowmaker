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


package test.utilities;


import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

public abstract class BaseParseField
    implements ParseField {

    // a handle to the parent resources
    protected ParseFieldManager parseFieldManager = null;

    // the token we get triggered by
    protected String  token;
    int   minValue;
    int   maxValue;
    boolean useCurrent = false;

  /**
   * we need a no-arg default constructor in order to be loaded by the
   * class loader
   */
  public BaseParseField() {
  }

  public String getToken() {
    return token;
  }

  public abstract void initialize(ParseFieldManager manager, Node t);

  /**
   * default implementation. This is meant to be overridden, but the code is left
   * here for example purposes
   */
/*
  public abstract void initialize(ParseFieldManager manager, Node t) {
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
    }
  }
*/

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

    String str = new String("default junk");

    return str;
  }

  public String toString() {
    return token;
  }

}

