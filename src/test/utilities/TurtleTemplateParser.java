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

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.nio.charset.StandardCharsets;

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


public class TurtleTemplateParser
    implements TemplateParser
{
  public static boolean DEBUG_FLAG = false;

  // need a random number generator
  static final Random randNum = new Random();
  SimpleDateFormat dateTimePreciseTimestampFormatter =
              new SimpleDateFormat("yyyy-MM-dd HH:mm:SS.SSSSSSS");

  ParseFieldManager fieldManager;

  protected HashMap<String, Object> contextDataHandle;

  public TurtleTemplateParser() {
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
  }

  /**
 * A simple Turtle parser
 *
 *
 @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
 @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
 @prefix swp: <http://www.w3.org/2004/03/trix/swp-1/> .
 @prefix dc: <http://purl.org/dc/elements/1.1/> .
 @prefix ex: <http://www.example.org/vocabulary#> .
 @prefix : <http://www.example.org/exampleDocument#> .
 
 :G1 { :Monica ex:name "Monica Murphy" .      
       :Monica ex:homepage <http://www.monicamurphy.org> .
       :Monica ex:email <mailto:monica@monicamurphy.org> .
       :Monica ex:hasSkill ex:Management }
 
 :G2 { :Monica rdf:type ex:Person .
       :Monica ex:hasSkill ex:Programming }
 
 :G3 { :G1 swp:assertedBy _:w1 .
       _:w1 swp:authority :Chris .
       _:w1 dc:date "2003-10-02"^^xsd:date .   
       :G2 swp:quotedBy _:w2 .
       :G3 swp:assertedBy _:w2 .
       _:w2 dc:date "2003-09-03"^^xsd:date .
       _:w2 swp:authority :Chris .
       :Chris rdf:type ex:Person .
       :Chris ex:email <mailto:chris@bizer.de> }

 *
 *
 *@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
 @prefix dc: <http://purl.org/dc/elements/1.1/> .
 @prefix ex: <http://example.org/stuff/1.0/> .
 
 <http://www.w3.org/TR/rdf-syntax-grammar>
   dc:title "RDF/XML Syntax Specification (Revised)" ;
   ex:editor [
     ex:fullname "Dave Beckett";
     ex:homePage <http://purl.org/net/dajobe/>
   ] .

*/

  class TurtleTokenizer {
    private ArrayList<String> tokens;
    private int lastToken;

    TurtleTokenizer(String str) {
      tokens = new ArrayList<String>();
      lastToken = -1;

      processString(str);
    }

    boolean hasMoreTokens() {
      if (tokens == null)
        return false;

      if (tokens.size() == 0)
        return false;

      if ((lastToken + 1) >= tokens.size())
        return false;

      return true;
    }

    String nextToken() {
      if (!hasMoreTokens())
        throw new NoSuchElementException("lastToken was " + lastToken);

      return tokens.get(++lastToken);
    }

    private boolean isWhitespace(char c) {
      if (c == ' ')
        return true;
      if (c == '\t')
        return true;

      return false;
    }

    private void processString(String str) {

      if (str == null)
        return;
      
      String pstr = str.trim();
      int pos = 0;
      int len = pstr.length();

      // System.out.println("pstr is '" + pstr + "'");
      // System.out.println("len is " + len);

      // handle the special cases

      while (pos < len) {
        // is it a prefix?
        if (pstr.charAt(pos) == '@') {
          int startPos = pos;
          while ((++pos < len) && !isWhitespace(pstr.charAt(pos)))
            ;
          String prefix = pstr.substring(startPos, pos);
          // System.out.println("adding token " + prefix);
          tokens.add(prefix);
          // now move around the next whitespace
/*
          while ((++pos < len) && isWhitespace(pstr.charAt(pos)))
            ;
*/
        }
        else if (pstr.charAt(pos) == '.') {
          // System.out.println("adding token '.'" );
          tokens.add(".");
          ++pos;
          // now move around the next whitespace
/*
          while ((++pos < len) && isWhitespace(pstr.charAt(pos)))
            ;
*/
        }
        else if (pstr.charAt(pos) == '<') {
          int startPos = pos;
          while ((++pos < len) && pstr.charAt(pos) != '>')
            ;
          String prefix = pstr.substring(startPos, ++pos);
          // System.out.println("adding token '" + prefix + "'");
          tokens.add(prefix);
          // now move around the next whitespace
/*
          while ((++pos < len) && isWhitespace(pstr.charAt(pos)))
            ;
*/
        }
        // a double quoted string will either be a literal with no type,
        // or will be a literal with a trailing type
        // either way, the entire token is good up to the last trailing whitespace
         else if (pstr.charAt(pos) == '\"') {
          int startPos = pos;
          // first we find the trailing double quote
          while ((++pos < len) && (pstr.charAt(pos) != '\"'))
            ;
          // now we have to find the end of the datatype, if there is one
          while ((++pos < len) && !isWhitespace(pstr.charAt(pos)))
            ;
          String prefix = pstr.substring(startPos, pos);
          // System.out.println("adding token " + prefix);
          tokens.add(prefix);
          // now move around the next whitespace
/*
          while ((++pos < len) && isWhitespace(pstr.charAt(pos)))
            ;
*/
        }

        // must be a plain text field - graph, {, ;, etc
         else if (!isWhitespace(pstr.charAt(pos))) {
          int startPos = pos;
          while ((++pos < len) && !isWhitespace(pstr.charAt(pos)))
            ;
          String prefix = pstr.substring(startPos, pos);
          // System.out.println("adding token " + prefix);
          tokens.add(prefix);
          // now move around the next whitespace
/*
          while ((++pos < len) && isWhitespace(pstr.charAt(pos)))
            ;
*/
        }

        // when all else is done, we hop past the whitespace
        while ((++pos < len) && isWhitespace(pstr.charAt(pos)))
          ;
      }

    }
    
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

  /**
   * something doesn't work well with quoted strings and embedded spaces
   */
  protected String trimToken(String token) {

    int leadPos = 0;
    int trailPos = -1;

    if (token == null)
      return null;

    boolean complete = false;
    while (!complete) {
      if (token.charAt(leadPos) == ' ')
        ++leadPos;
      else
        complete = true;
    }

    trailPos = token.length() - 1;
    complete = false;
    while (!complete) {
      if (token.charAt(trailPos) == ' ')
        --trailPos;
      else
        complete = true;
    }

    return token.substring(leadPos, trailPos+1);
  }

  protected String handleToken(String inpiece) {
    String rstr = null;

    rstr = inpiece;
    String trimmedToken = trimToken(inpiece);

    // System.out.println("handleToken:  '" + trimmedToken + "'");

    // is it surrounded by double quotes?
    if ((trimmedToken.charAt(0) == '\"') && (trimmedToken.charAt(inpiece.length()-1) == '\"')) {
      // System.out.println("trimmed token framed quotes is '" + trimmedToken + "'");
      String str = trimmedToken.substring(1, trimmedToken.length()-1);
      String str2 = parseField(str);
      rstr = "\"" + str2 + "\"";
    }

    // is it a value surrounded by double quotes with a datatype?
    else if (trimmedToken.charAt(0) == '\"') {
      // System.out.println("trimmed token is '" + trimmedToken + "'");
      // find the trailing double quote
      int pos = trimmedToken.indexOf('\"', 1);
      String value = trimmedToken.substring(1, pos);
      String datatype = trimmedToken.substring(pos+1);
      // System.out.println("trimmed token value is '" + value + "'");
      // we might have to do something here to deal with embedded double quotes? Dunno
      rstr = "\"" + parseField(value) + "\"" + datatype;
    }

    // is it an IRI?
    else if (trimmedToken.indexOf(':') > -1) {
      // make an assumption that the portion before the colon can't be generated
      // System.out.println("I have an IRI");
      String prefix = trimmedToken.substring(0, trimmedToken.indexOf(':'));
      String suffix = trimmedToken.substring(trimmedToken.indexOf(':')+1);
      rstr = prefix + ":" + parseField(suffix);
    }

    // deal with the default situation
    else {
      // System.out.println("in default handling");
      rstr = parseField(trimmedToken);
    }

    return rstr;
  }

  public void parseTemplate(InputStream instream, OutputStream outstream) {

    boolean complete = false;

    if ((instream == null) || (outstream == null))
      return;

    BufferedInputStream bis = new BufferedInputStream(instream);
    BufferedOutputStream bos = new BufferedOutputStream(outstream);
    PrintWriter pw = new PrintWriter(new OutputStreamWriter(bos));

    BufferedReader br = new BufferedReader( new InputStreamReader(bis, StandardCharsets.UTF_8));

    try {


      while (!complete) {

      StringBuffer sb = new StringBuffer();

      // get a line at a time
        String line = br.readLine();
        if (line == null)
          complete = true;
        else {
      // get a token at a time
        // System.out.println("line coming in is: '" + line + "'");
        TurtleTokenizer tokens = new TurtleTokenizer(line);
        int counter = 0;
        while (tokens.hasMoreTokens()) {
          if (counter++ > 0)
            sb.append(" ");
          String token = tokens.nextToken();
          String str = handleToken(token);
          sb.append(str);
        }
        }
        String nline = sb.toString();
        pw.println(nline);
        // for now, let's get what we have
        pw.flush();
      }

      pw.flush();

    }
    catch (IOException e) {
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
   * usage:  test.stress.TurtleTemplateParser config_file template_file <output_file>
   */

  public static void main(String[] args) {

    TurtleTemplateParser tester = new TurtleTemplateParser();
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


