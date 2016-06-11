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


package test.utilities;

import java.io.PrintStream;

import java.util.Random;
import java.util.HashMap;
import java.util.Iterator;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.w3c.dom.Node;



public class BaseParseFieldManager
      implements ParseFieldManager {


    /**
     * For whatever reason, there are problems on pre-1.8 classloaders that resort
     * in retaining weak references to classes and eventually exhausting heap space.
     * In order to minimize this problem, a static internal map is being used to prevent
     * more class loading than is necessary during the running of the parser. There is
     * no fix to the problem short of replacing the classloader for the VM, but work
     * can be done to minimize the impact. Even with this change, if the run goes on
     * long enough, it will terminate with an OOM error. The problem is not ours,
     * but we can make it better.
     */
    private static HashMap<String, Class> classHash = new HashMap<String, Class>();

    // enable this to see token processing
    private static final boolean debugFlag = false;
    // enable this to see classloader hash processing
    private static final boolean classLoaderDebugFlag = false;



    Random randNum = null;
    HashMap<String, ParseField> fields = null;

    public BaseParseFieldManager() {
      randNum = new Random();
      fields = new HashMap<String, ParseField>();
    }

    public Random getRandomNumberGenerator() {
      return randNum;
    }

  public ParseField getParseField(String tag) {
    ParseField field = fields.get(tag);

    return field;
  }

  /**
   * allow a module to add a field that is more under its own control
   */
  public void addCustomField(String token, ParseField field) {
    if ((token == null)  || (field == null))
      return;

    field.initialize(this, (Node)null);
    if (field.getToken() == null) {
      System.out.println("Field is malformed:");
      Throwable t = new Throwable("field has a null token");
      t.printStackTrace();
    }

    fields.put(token, field);
  }

  public void loadCustomField(Node t) {
    String tmp;

    if ((t == null) || (t.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE))
      return;
    NodeList children = t.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      if (children.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      String tagName = ((Element) children.item(i)).getTagName();
      if (tagName.equals("class")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals(""))) {
          String className = tmp;
          try {
            if (classLoaderDebugFlag)
              System.out.println("loading class " + className);
            Class c = null;
            synchronized(classHash) {
              c = classHash.get(className);
              if (c == null) {
                c = Class.forName(className);
                classHash.put(className, c);
                if (classLoaderDebugFlag)
                  System.out.println("adding to classHash for " + className);
              } else {
                if (classLoaderDebugFlag)
                  System.out.println("found existing classHash for " + className);
              }
            }
            Object o = c.newInstance();
            ParseField field = (ParseField)o;
            field.initialize(this, t);
            String token = field.getToken();
            if (debugFlag)
              System.out.println("token is " + token);
            if (token == null) {
              System.out.println("ERROR:  null token for className " + className);
            }
            fields.put(token, field);
          }
          catch (ClassNotFoundException e) {
            e.printStackTrace();
          }
          catch (InstantiationException e) {
            e.printStackTrace();
          }
          catch (IllegalAccessException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  public void initialize(Node t) {
    String tmp;

    if ((t == null) || (t.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE))
      return;
    NodeList children = t.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      if (children.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      String tagName = ((Element) children.item(i)).getTagName();
      if (tagName.equals("custom-field")) {
        loadCustomField(children.item(i));
      }
    }
  }

  /**
   * utility function to allow us to see what is configured in the field manager
   */
  public void dumpFieldList(PrintStream ps) {
    Iterator iterator = fields.values().iterator();

    ps.println("ParseFieldManager fields:");
    while (iterator.hasNext()) {
      ParseField field = (ParseField)iterator.next();
      ps.println(field.getToken());
    }
    
    ps.println("ParseFieldManager classes:");
    iterator = classHash.keySet().iterator();
    while (iterator.hasNext()) {
      String className = (String)iterator.next();
      ps.println(className);
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



}

