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

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;
import org.w3c.dom.Attr;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.SAXException;


public class BaseTemplateParser
    implements TemplateParser
{
  public static boolean DEBUG_FLAG = false;

  // need a random number generator
  static final Random randNum = new Random();
  SimpleDateFormat dateTimePreciseTimestampFormatter =
              new SimpleDateFormat("yyyy-MM-dd HH:mm:SS.SSSSSSS");

  ParseFieldManager fieldManager;

  protected DocumentBuilder loadBuilder;
  protected DocumentBuilder saveBuilder;
  protected Document loadDocument;
  protected Document saveDocument;
  protected HashMap<String, Object> contextDataHandle;

  public BaseTemplateParser() {
    contextDataHandle = new HashMap<String, Object>();
  }

  public void setDebugFlag(boolean flag) {
    DEBUG_FLAG = flag;
  }

  public void
  debugLog(String str) {
    if (DEBUG_FLAG)
      System.out.println(str);
  }

  public HashMap getContextDataHandle() {
    return contextDataHandle;
  }

  public void
  setContextDataHandle(HashMap handle) {
    contextDataHandle = handle;
  }

  /**
   * does nothing right now, but here for symmetry with cleanup
   */
  public void
  initialize() {
  }

  /**
   * used for testing purposes. Loads a config file and initializes the parse field
   * manager with the information for the test template load.
   */
  public void
  initialize(String configFileName) {
    File file = null;
    FileInputStream is = null;

    if (configFileName == null)
      return;

    try {
      file = new File(configFileName);
      is = new FileInputStream(file);
    } catch (FileNotFoundException e) {
      debugLog("config file not found:  " + configFileName);
      e.printStackTrace();
      return;
    }

    DocumentBuilderFactory dbFactory;
    DocumentBuilder dBuilder;
    Document doc = null;

    try {
      dbFactory = DocumentBuilderFactory.newInstance();
      dBuilder = dbFactory.newDocumentBuilder();
      doc = dBuilder.parse(is);


      // now we need to walk the configuration
      NodeList configList = doc.getDocumentElement().getChildNodes();
      for (int i = 0; i < configList.getLength(); i++) {
        if (configList.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
          continue;
        }
        String tagName = ((Element)configList.item(i)).getTagName();
        if (tagName.equalsIgnoreCase("custom-fields")) {
          fieldManager.initialize(configList.item(i));
        }
      }
    }
    catch (ParserConfigurationException e) {
      debugLog("making config document");
      e.printStackTrace();
    }
    catch (SAXException e) {
      debugLog("Exception parsing config");
      e.printStackTrace();
    }
    catch (IOException e) {
      debugLog("Exception parsing config");
      e.printStackTrace();
    }
    finally {
      try {
        is.close();
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void
  setFieldManager(ParseFieldManager manager) {
    fieldManager = manager;
  }

  /**
   * clean up objects, including resetting the document builders to see if
   * that clears the zip handles
   */
  public void
  cleanup() {
    /*
     * JDK 1.8 doesn't like this call
    if (loadBuilder != null)
      loadBuilder.reset();
    if (saveBuilder != null)
      saveBuilder.reset();
    */
    loadDocument = null;
    saveDocument = null;
    loadBuilder = null;
    saveBuilder = null;
  }

  /**
   * parse the template entry and perform substitutions:
   *  __DATE_NOW__
   * AAAAAAAAAA
   * aaaaaaaaaa
   * NNNNNNNNN
   * AAAaaaaaNNNNnnnn
   * __TIME_NOW__
   * __DATETIME_NOW__
   * __DATE__
   * __TIME__
   * __DATETIME__
   */

  static final String upperAlpha = new String("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
  static final String lowerAlpha = new String("abcdefghijklmnopqrstuvwxyz");
  static final String numbers = new String("0123456789");

  
  public String parseField(String value) {

    if (value == null)
      return null;

    if (value.length() == 0)
      return value;

    // TODO
    if ((value.equals("__DATETIME_PRECISE_TIMESTAMP__"))
      || (value.equals("YYYY-MM-DDTHH:MM:SS.NNNNNNN+NN:NN"))) {
      long variation = randNum.nextLong() % 86400000L;
      Date d = new Date(System.currentTimeMillis() + variation);
      String s = dateTimePreciseTimestampFormatter.format(d);
      s = s + "+";
      s = s + Integer.toString(randNum.nextInt(6));
      s = s + Integer.toString(randNum.nextInt(6));
      s = s + ":";
      s = s + Integer.toString(randNum.nextInt(6));
      s = s + Integer.toString(randNum.nextInt(6));
      return s;
    }

    ParseField field;
    field = fieldManager.getParseField(value);
    if (field != null) {
      String s = field.generateData(value, contextDataHandle);

      return s;
    }
    
    // handle each type individually?
    // sniff the value to see if it needs substitution
    boolean isSpecial = true;
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if ((c == 'A') || (c == 'a') || (c == '.') ||
        (c == '-') || (c == '_') || (c == ' ') ||
        (c == '/') || 
        (c == 'N') || (c == 'n')) {
      } else {
        isSpecial = false;
        // heck, just get out of here
        return value;
      }
    }
    
    StringBuffer buf = new StringBuffer();
    int r;

    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);

      switch (c) {
        case 'A':
          r = -1;
          while (r < 0) {
            r = randNum.nextInt() % 26;
          }
          buf.append(upperAlpha.charAt(r));
          break;

        case 'a':
          r = -1;
          while (r < 0) {
            r = randNum.nextInt() % 26;
          }
          buf.append(lowerAlpha.charAt(r));
          break;

        case 'N':
          r = -1;
          while (r < 0) {
            r = randNum.nextInt() % 10;
          }
          buf.append(numbers.charAt(r));
          break;

        case 'n':
          r = -1;
          while (r < 0) {
            r = randNum.nextInt() % 10;
          }
          buf.append(numbers.charAt(r));
          break;

        default:
          buf.append(c);
          break;
      }
    }

    String str = new String(buf);
    return str;
  }

  public void listAllAttributes(Element srcElement, Element destElement, int level) {
    String nodeIndent;
    String indent = new String();
    for (int ii = 0; ii < level; ii++)
      indent = indent.concat("  ");

    nodeIndent = indent + "  ";
    debugLog(nodeIndent + "List attributes for node: " + srcElement.getNodeName());
    // get a map containing the attributes of this node
    NamedNodeMap attributes = srcElement.getAttributes();
    // get the number of nodes in this map
    int numAttrs = attributes.getLength();

    for (int i = 0; i < numAttrs; i++) {
      Attr attr = (Attr) attributes.item(i);
      String attrName = attr.getNodeName();
      String attrValue = attr.getNodeValue();
      debugLog(nodeIndent + "Found attribute: " + attrName + " with value: " + attrValue);
      attrValue = parseField(attrValue);
      Attr newAttr = saveDocument.createAttribute(attrName);
      newAttr.setValue(attrValue);
      destElement.setAttributeNode(newAttr);
    }
  }


	private String
	getElementValue(String tag,  Element element)
	{
		if (element == null)
			debugLog("getElementValue:  element is null");

		// System.out.println("getElementValue:  looking for " + element.getNodeName());

		// NodeList nlList = element.getElementsByTagName(tag).item(0).getChildNodes();

		NodeList nlList = element.getChildNodes();
		int ct = nlList.getLength();
		// System.out.println("getElementValue:  childNode count is " + ct);
		if (nlList == null)
		{
			// System.out.println("null nlList returned looking for " + tag);
			return null;
		}

		Node nValue = (Node)nlList.item(0);
		if (nValue == null)
		{
			// it looks like empty elements return a null nValue here
			// System.out.println("null nValue returned looking for " + tag);
			return null;
		}

		return nValue.getNodeValue();
	}


	private String
	getTagValue(String tag, Element element)
	{
		debugLog("getTagValue:  looking for " + tag);

		if (element == null)
			debugLog("getTagValue:  element is null");

		// NodeList nlList = element.getElementsByTagName(tag).item(0).getChildNodes();

		NodeList elList = element.getElementsByTagName(tag);
		if (elList == null)
		{
			debugLog("null elList returned looking for " + tag);
			return null;
		}
		// debugLog("elList for " + tag + " is length " + elList.getLength());

		Node n = elList.item(0);
		if (n == null)
		{
			debugLog("node n for elList.item(0) is null");
			return null;
		}

		NodeList nlList = n.getChildNodes();
		int ct = nlList.getLength();
		debugLog("getTagValue:  childNode count is " + ct);
		if (nlList == null)
		{
			debugLog("null nlList returned looking for " + tag);
			return null;
		}

		Node nValue = (Node)nlList.item(0);
		if (nValue == null)
		{
			// it looks like empty elements return a null nValue here
			debugLog("null nValue returned looking for " + tag);
			return null;
		}

		return nValue.getNodeValue();
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
        if (!isWhitespaceNode(child))
          text += child.getNodeValue();
    }
    return text;
  }

	void
	walkDoc(Element fromElement, Element toElement, int level)
	{
		String indent;
		String nodeIndent;

		indent = new String("");
		debugLog("walkDoc:  level is " + level);
		for (int ii = 0; ii < level; ii++)
			indent = indent.concat("  ");

		nodeIndent = indent + "  ";

		debugLog(indent + "walkDoc");
		if (fromElement == null)
		{
			debugLog("walkDoc:  somehow element is null");
			return;
		}
		debugLog( indent + "Node name:  " + fromElement.getNodeName());
		// debugLog(indent + "Node value:  " + getTagValue(fromElement.getNodeName(), fromElement));
		debugLog(indent + "Node value:  " + getElementValue(null, fromElement));

    listAllAttributes(fromElement, toElement, level);

		NodeList nodes = fromElement.getChildNodes();
		int ct = nodes.getLength();
		debugLog(indent + "Number of children:  " + ct);
		for (int ii = 0; ii < ct; ii++)
		{
			Node n = nodes.item(ii);
			if (n instanceof Element)
			{

        // need to make a new document, then pass it in
        debugLog("We have a new element, level:  " + n.getNodeName() + ", " + level);
        Element newElement = saveDocument.createElement(n.getNodeName());
        String value = n.getNodeValue();
        String text = getNodeText((Element)n);
        debugLog("node value before parseField is " + value);
        debugLog("node text before parseField is " + text);
        value = parseField(value);
        text = parseField(text);
        // debugLog("node text after parseField is " + text);
        if (value != null) {
          newElement.appendChild(saveDocument.createTextNode(value));
        } else {
          if (text != null)
            newElement.appendChild(saveDocument.createTextNode(text));
        }
        toElement.appendChild(newElement);

				walkDoc((Element)n, newElement, level + 1);
			}
      else if (n instanceof Text)
      {
        debugLog("walkDoc:  this is of type Text at level " + level);
				debugLog(nodeIndent + ii + " Node name:  " + n.getNodeName());
				debugLog(nodeIndent + ii + " Node value:  " + n.getNodeValue());
      }
			else
			{
        debugLog("walkDoc:  do we need to do something about this?");
				debugLog(nodeIndent + ii + " Node name:  " + n.getNodeName());
				debugLog(nodeIndent + ii + " Node value:  " + n.getNodeValue());
			}
		}
		// we think we're on an element, so we get the value for it here?
		// String str = getElementValue(null, el);
		// debugLog(indent + "Is this the element value?  " + str);
	}

	void
	walkDocument(Document loadDoc, Document saveDoc, int level)
	{
		Element loadRoot;
    Element saveRoot;
		String indent;
		String nodeIndent;

		indent = new String();
		for (int ii = 0; ii < level; ii++)
			indent = indent.concat("  ");

		nodeIndent = indent + "  ";

		debugLog("walkDocument");
		if (loadDoc == null)
		{
			debugLog("somehow loadDoc is null");
			return;
		}

		loadRoot = loadDoc.getDocumentElement();
    saveRoot = saveDoc.getDocumentElement();
		debugLog(indent + "Node name:  " + loadRoot.getNodeName());
		debugLog(indent + "Node value:  " + getElementValue(loadRoot.getNodeName(), loadRoot));

    listAllAttributes(loadRoot, saveRoot, level);

		NodeList nodes = loadRoot.getChildNodes();
		int ct = nodes.getLength();
		debugLog("Total children for root document:  " + ct);
		for (int ii = 0; ii < ct; ii++)
		{
			Node n = nodes.item(ii);
			if (n instanceof Element)
			{
        // need to make a new document, then pass it in
        Element newElement = saveDoc.createElement(n.getNodeName());
        String value = n.getNodeValue();
        String text = getNodeText((Element)n);
        debugLog("node value before parsing is " + value);
        value = parseField(value);
        debugLog("node value after parsing is " + value);
        text = parseField(text);
        debugLog("node text after parsing is " + text);
        
        if (value != null)
          newElement.appendChild(saveDoc.createTextNode(value));
        else {
          if (text != null)
            newElement.appendChild(saveDoc.createTextNode(text));
        }
        saveRoot.appendChild(newElement);

				walkDoc((Element)n, newElement, level + 1);
			}
			else
			{
        debugLog("walkDocument:  do we need to do something about this?");
				debugLog(nodeIndent + ii + " Node name:  " + n.getNodeName());
				debugLog(nodeIndent + ii + " Node value:  " + n.getNodeValue());
			}
		}
	}

	void
	parseDoc()
	{
		File file = null;
		InputStream is = null;

    String filename = new String("");

		try
		{
			file = new File(filename);
			is = new FileInputStream(file);
		}
		catch (FileNotFoundException e)
		{
			debugLog("File not found:  " + filename);
			e.printStackTrace();
			return;
		}

		// we have the response. Now let's turn it into a DOM object

		DocumentBuilderFactory dbFactory;
		DocumentBuilder dBuilder;
		Document doc = null;

		try
		{
			dbFactory = DocumentBuilderFactory.newInstance();
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
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }

		// now parse the document into the object - let it do itself
		// walkDocument(doc, 0);
	}

  public void parseTemplate(InputStream instream, OutputStream outstream) {

    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      loadBuilder = factory.newDocumentBuilder();

      saveBuilder = factory.newDocumentBuilder();
      saveDocument = saveBuilder.newDocument();

      loadDocument = loadBuilder.parse(instream);
      Element rootElement = loadDocument.getDocumentElement();
      Element newRoot = saveDocument.createElement(rootElement.getTagName());
      saveDocument.appendChild(newRoot);
      walkDocument(loadDocument, saveDocument, 0);

      // adfdasfdaf
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      Transformer transformer = transformerFactory.newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      DOMSource source = new DOMSource(saveDocument);
      StreamResult result = new StreamResult(outstream);

      transformer.transform(source, result);

    }
    catch (IOException e) {
      e.printStackTrace();
    }
    catch (ParserConfigurationException e) {
      e.printStackTrace();
    }
    catch (TransformerException e) {
      e.printStackTrace();
    }
    catch (SAXException e) {
      e.printStackTrace();
    }
  }


  /**
   * temporary just to get the code written. This needs to take a buffer
   */
  public void parseTemplateFile(String templateFile, String outputFile) {
    
    File inputFile = null;
    FileInputStream is = null;
    File outFile = null;
    FileOutputStream os = null;

    try {

    inputFile = new File(templateFile);
    if (!inputFile.canRead()) {
      System.out.println("parseTemplateFile:  can't read input file "
                            + templateFile);
    }

    System.out.println("parsing template " + inputFile);

    is = new FileInputStream(inputFile);

    outFile = new File(outputFile);
    os = new FileOutputStream(outFile);

    parseTemplate(is, os);

    } catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      try {
        os.flush();
        os.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      try {
        is.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }


  /**
   * temporary just to get the code written. This needs to take a buffer
   */
  public void parseTemplateFile(String templateFile, OutputStream outstream) {
    
  FileInputStream is = null;

  try {

    File inputFile = new File(templateFile);
    if (!inputFile.canRead()) {
      System.out.println("unable to open template for read:  " 
                            + templateFile);
    }

      is = new FileInputStream(inputFile);
      parseTemplate(is, outstream);

    } catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      try {
        is.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }


  /**
   * usage:  test.stress.BaseTemplateParser config_file template_file <output_file>
   */

  public static void main(String[] args) {

    BaseTemplateParser tester = new BaseTemplateParser();
    BaseParseFieldManager fm = new BaseParseFieldManager();
    tester.setFieldManager(fm);

    tester.initialize(args[0]);

    if (args.length < 3)
      tester.parseTemplateFile(args[1], System.out);
    else
      tester.parseTemplateFile(args[1], args[2]);

    System.exit(0);
  }
}


