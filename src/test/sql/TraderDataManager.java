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
import java.io.IOException;
import java.io.FileNotFoundException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

import org.joda.time.DateTime;

import test.utilities.MarkLogicWrapper;
import test.utilities.MarkLogicWrapperFactory;
import test.utilities.PrefixMapper;
import test.utilities.GraphUtils;

import test.stress.StressTester;
import test.stress.StressTestProperties;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


public class TraderDataManager {
  protected MarkLogicWrapper wrapper = null;

  protected static final boolean debugFlag = false;

  public static final String LINKAGE_GRAPH_NAME = "link:traders";
  public static final String LINKAGE_SUBJECT_PREFIX = "link:Trader_";
  public static final String LINKAGE_TRADER_ID_PREDICATE = "link:TraderId";
  public static final String LINKAGE_RATING_PREDICATE = "link:Rating";
  public static final String LINKAGE_TRADE_HOUSE_PREDICATE = "link:TradeHouse";
  public static final String LINKAGE_NAME_PREDICATE = "link:Name";

  protected String host;
  protected int port;
  protected String user;
  protected String password;
  protected String database;

  protected PrefixMapper prefixMapper;


  private static TraderDataManager thisTracker = null;
  private boolean isInitialized = false;
  private Random randNum;

  private TraderDataManager() {
    prefixMapper = PrefixMapper.getPrefixMapper();
    randNum = new Random();
  }

  public static TraderDataManager getTracker() {

    if (thisTracker == null) {
      thisTracker = new TraderDataManager();
      thisTracker.initialize();
    }

    return thisTracker;
  }

  public void initialize() {

    if (isInitialized)
      return;

    prefixMapper.addPrefix( "xs", "http://www.w3.org/2001/XMLSchema#");
    prefixMapper.addPrefix("link", "http://marklogic.com/stress/tde/linkage/dbts#");

    StressTestProperties props = StressTestProperties.getStressTestProperties();
    String configInfo = props.getPropertyAsPath("sql.verification.trackingdata.config");

    // parse the config file now to get the connection information
    parseConfigInfo(configInfo);

    isInitialized = true;
  }

  public void cleanup() {
    flush();
  }

  private void parseConfigInfo(String filename) {

    if (filename == null) {
      System.out.println("TraderDataManager:  no config file property present");
      return;
    }

    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document connectDocument = builder.parse(new File(filename));
      Element rootElement = connectDocument.getDocumentElement();
      NodeList nlist = null;
      nlist = rootElement.getElementsByTagName("host");
      if (nlist == null) {
        System.out.println("TraderDataManager:  no host entry in configuratiion");
      } else {
        Element hostElement = (Element)nlist.item(0);
        if (hostElement != null) {
          host = hostElement.getTextContent();
          System.out.println("TraderDataManager:  host is " + host);
        } else {
          System.out.println("TraderDataManager:  host is empty");
        }
      }

      nlist = rootElement.getElementsByTagName("port");
      if (nlist == null) {
        System.out.println("TraderDataManager:  no port entry in configuratiion");
      } else {
        Element portElement = (Element)nlist.item(0);
        if (portElement != null) {
          port = Integer.parseInt(portElement.getTextContent());
          System.out.println("TraderDataManager:  port is " + port);
        } else {
          System.out.println("TraderDataManager:  port is empty");
        }
      }

      nlist = rootElement.getElementsByTagName("username");
      if (nlist == null) {
        System.out.println("TraderDataManager:  no username entry in configuratiion");
      } else {
        Element usernameElement = (Element)nlist.item(0);
        if (usernameElement != null) {
          user = usernameElement.getTextContent();
          System.out.println("TraderDataManager:  username is " + user);
        } else {
          System.out.println("TraderDataManager:  username is empty");
        }
      }

      nlist = rootElement.getElementsByTagName("password");
      if (nlist == null) {
        System.out.println("TraderDataManager:  no password entry in configuratiion");
      } else {
        Element passwordElement = (Element)nlist.item(0);
        if (passwordElement != null) {
          password = passwordElement.getTextContent();
          System.out.println("TraderDataManager:  password is " + password);
        } else {
          System.out.println("TraderDataManager:  password is empty");
        }
      }

      nlist = rootElement.getElementsByTagName("database");
      if (nlist == null) {
        System.out.println("TraderDataManager:  no database entry in configuratiion");
      } else {
        Element databaseElement = (Element)nlist.item(0);
        if (databaseElement != null) {
          database = databaseElement.getTextContent();
          System.out.println("TraderDataManager:  database is " + database);
        } else {
          System.out.println("TraderDataManager:  database is empty");
        }
      }
    } catch (FileNotFoundException e) {
      System.out.println("TraderDataManager:  file not found - " + filename);
    } catch (IOException e) {
      System.out.println("TraderDataManager:  file error - " + filename);
    } catch (ParserConfigurationException e) {
      e.printStackTrace();
    } catch (SAXException e) {
      e.printStackTrace();
    }

  }

  private void setConnectionInfo(String host,
                                  int port,
                                  String user,
                                  String password,
                                  String database) {

    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.database = database;
  }

  public void connect() {

    // temporary to make sure we get initialized properly
    //
/*
    host = "rh7-intel64-80-qa-dev-5";
    port = 8020;
    user = "admin";
    password = "admin";
*/

    if (wrapper == null) {
      wrapper = MarkLogicWrapperFactory.getWrapper(host,
                                                    port,
                                                    user,
                                                    password,
                                                    null);
    }

  }

  public void flush() {
    if (wrapper != null)
      wrapper.flush();
  }

  public void disconnect() {
    if (wrapper != null)
      wrapper.flush();
/*
    if (wrapper != null)
      wrapper.disconnect();
*/
  }

  public String makeSubject() {
    // String sub = LINKAGE_SUBJECT_PREFIX + StressTester.randomString(8);

    DateTime now = new DateTime();
    int year, month, day, hour, minute, second;
    year = now.year().get();
    month = now.monthOfYear().get();
    day = now.dayOfMonth().get();
    hour = now.getHourOfDay();
    minute = now.getMinuteOfHour();
    second = now.getSecondOfMinute();

    StringBuilder sb = new StringBuilder();
    sb.append(LINKAGE_SUBJECT_PREFIX);
    sb.append(StressTester.randomString(8));
    sb.append("_");
    sb.append(Integer.toString(year));
    if (month < 10)
      sb.append("0");
    sb.append(Integer.toString(month));
    if (day < 10)
      sb.append("0");
    sb.append(Integer.toString(day));
    sb.append("_");
    if (hour < 10)
      sb.append("0");
    sb.append(Integer.toString(hour));
    if (minute < 10)
      sb.append("0");
    sb.append(Integer.toString(minute));
    if (second < 10)
      sb.append("0");
    sb.append(Integer.toString(second));

    return sb.toString();
  }


  protected void storeTrackingData(String traderId, String tradeHouse, String name, String rating) {

    // get the wrapper
    if (wrapper == null) {
      connect();
    }

    ArrayList<Triple> triples = new ArrayList<Triple>();

    // concoct a subject
    String sub = makeSubject();

    Node subject = NodeFactory.createURI(sub);
    Node predicate = NodeFactory.createURI(LINKAGE_TRADER_ID_PREDICATE);
    Node object = NodeFactory.createLiteral(traderId);

    Triple triple = Triple.create(subject, predicate, object);
    triples.add(triple);

    predicate = NodeFactory.createURI(LINKAGE_RATING_PREDICATE);
    object = NodeFactory.createLiteral(rating);

    triple = Triple.create(subject, predicate, object);
    triples.add(triple);

    predicate = NodeFactory.createURI(LINKAGE_TRADE_HOUSE_PREDICATE);
    object = NodeFactory.createLiteral(tradeHouse);

    triple = Triple.create(subject, predicate, object);
    triples.add(triple);

    predicate = NodeFactory.createURI(LINKAGE_NAME_PREDICATE);
    object = NodeFactory.createLiteral(name);

    triple = Triple.create(subject, predicate, object);
    triples.add(triple);


    // create the query
    StringBuilder sb = new StringBuilder();
    sb.append(prefixMapper.getPrefixesAsString());
    sb.append("\n");
    sb.append("INSERT DATA {");
    sb.append("\n");
    sb.append("  GRAPH ");
    sb.append(LINKAGE_GRAPH_NAME);
    sb.append(" { \n");
    sb.append("    ");
    sb.append(subject);
    sb.append(" ");
    sb.append(LINKAGE_TRADER_ID_PREDICATE);
    sb.append(" \"");
    sb.append(traderId);
    sb.append("\"^^xs:int . ");
    sb.append("\n");
    sb.append("    ");
    sb.append(subject);
    sb.append(" ");
    sb.append(LINKAGE_RATING_PREDICATE);
    sb.append(" \"");
    sb.append(rating);
    sb.append("\" . ");
    sb.append("\n");
    sb.append("    ");
    sb.append(subject);
    sb.append(" ");
    sb.append(LINKAGE_TRADE_HOUSE_PREDICATE);
    sb.append(" \"");
    sb.append(tradeHouse);
    sb.append("\"^^xs:int . ");
    sb.append("\n");
    sb.append("    ");
    sb.append(subject);
    sb.append(" ");
    sb.append(LINKAGE_NAME_PREDICATE);
    sb.append(" \"");
    sb.append(name);
    sb.append("\" . ");
    sb.append("\n");
    sb.append("  }");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");

    String query = sb.toString();

    if (debugFlag)
      System.out.println(query);

    GraphUtils.updateTripleQuery(wrapper, query);

  }


  /**
   * decide how many documents we should update
   */
  protected long getWorkCount() {
    StringBuilder sb = new StringBuilder();

    // get the wrapper
    if (wrapper == null) {
      connect();
    }

    sb.append(prefixMapper.getPrefixesAsString());
    sb.append("\n");

    sb.append("CONSTRUCT {");
    sb.append("\n");
    sb.append("link:Status link:total_traders ?total_uris .");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");
    sb.append("WHERE {");
    sb.append("\n");
    
    sb.append("SELECT (count(?s) as ?total_uris)");
    sb.append("\n");
    sb.append("  WHERE");
    sb.append("\n");
    sb.append("  {");
    sb.append("\n");
    sb.append("    GRAPH " + LINKAGE_GRAPH_NAME + " { ");
    sb.append("\n");
    sb.append("      ?s " + LINKAGE_TRADER_ID_PREDICATE + " ?uri .");
    sb.append("\n");
    sb.append("    }");
    sb.append("\n");
    sb.append("  }");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");

    String query = sb.toString();


    // run the query
    

    long count = 0;

    // get the count from the response
    ArrayList<Triple> triples = GraphUtils.constructTriplesForQuery(wrapper, query);
    Iterator iter = triples.iterator();
    while (iter.hasNext()) {
      Triple triple = (Triple)iter.next();
      Node subject = triple.getSubject();
      Node predicate = triple.getPredicate();
      Node object = triple.getObject();
      String sub = subject.toString();

      // System.out.println("triple:  " + triple.toString());

      String pred = predicate.toString();
      // this is going to be bad
      String obj = object.toString();

      // System.out.println("predicate:  " + pred);
      // System.out.println("object:  " + object);

      RDFDatatype datatype;

      String simplePred = prefixMapper.simplifyWithNamespace(pred);

      if (simplePred.equals("link:total_traders"))

        if (object.isLiteral()) {
          // Literal literal = object.asLiteral();
          datatype = object.getLiteralDatatype();
          if (datatype == null) {
            System.out.println("this darned thing must be a string");
          } else {
            if (datatype.equals(XSDDatatype.XSDunsignedLong)) {
              Integer l = (Integer)object.getLiteralValue();
              count = (long)l.intValue();
              // System.out.println("processing XSDunsignedLong:  Long, count = " + l + ", " + count);
            }
            String dUri = datatype.getURI();
            // System.out.println("dUri is " + dUri);
            // String str = literal.getString();
            // System.out.println("str is " + str);
          }
        }

        // count = Long.parseLong(obj);
    }

    return count;
  }

  /**
   * decide which documents to update
   */
  protected HashMap<String, TraderRecord> generateWorkload(int numRecords)
    throws Exception {


    HashMap<String, TraderRecord> updateList = new HashMap<String, TraderRecord>();

    int limit = numRecords;
    // temporary for debugging
    // limit = 10;

    System.out.println("generateWorkload:  limit is set to " + limit);

    // get the wrapper
    if (wrapper == null) {
      connect();
    }

    StringBuilder sb = new StringBuilder();

    sb.append(prefixMapper.getPrefixesAsString());
    sb.append("\n");

    sb.append("\n");
    sb.append("CONSTRUCT {");
    sb.append("\n");
    sb.append("  ?sub link:TraderId ?traderId .");
    sb.append("\n");
    sb.append("  ?sub link:Rating ?rating .");
    sb.append("\n");
    sb.append("  ?sub link:TradeHouse ?tradeHouse .");
    sb.append("\n");
    sb.append("  ?sub link:Name ?name .");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");
    sb.append("WHERE");
    sb.append("\n");
    sb.append("{");
    sb.append("\n");
    sb.append("  GRAPH " + LINKAGE_GRAPH_NAME + " { " );
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_TRADER_ID_PREDICATE + " ?traderId .");
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_RATING_PREDICATE + " ?rating .");
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_TRADE_HOUSE_PREDICATE + " ?tradeHouse .");
    sb.append("\n");
    sb.append("    OPTIONAL { ?sub " + LINKAGE_NAME_PREDICATE + " ?name . } ");
    sb.append("\n");
    sb.append("  }");
    sb.append("\n");


    sb.append("  {");
    sb.append("\n");
    sb.append("    select ?sub");
    sb.append("\n");
    sb.append("    where");
    sb.append("\n");
    sb.append("    {");
    sb.append("\n");
    sb.append("      graph " + LINKAGE_GRAPH_NAME + " {");
    sb.append("\n");
    sb.append("        ?sub link:TraderId ?thisId .");
    sb.append("\n");
    sb.append("      }");
    sb.append("\n");
    sb.append("    }");
    sb.append("\n");
    sb.append("    order by rand()");
    sb.append("\n");
    sb.append("    limit " + limit);
    sb.append("\n");
    sb.append("  }");
    sb.append("\n");

    sb.append("}");
    sb.append("\n");

    String query = sb.toString();

    System.out.println(query);

    ArrayList<Triple> triples = GraphUtils.constructTriplesForQuery(wrapper, query);
    Iterator iter = triples.iterator();
    while (iter.hasNext()) {
      Triple triple = (Triple)iter.next();
      Node subject = triple.getSubject();
      Node predicate = triple.getPredicate();
      Node object = triple.getObject();
      String sub = subject.toString();

      // System.out.println("triple:  " + triple.toString());

      // blah blah
      TraderRecord record = (TraderRecord)updateList.get(sub);
      if (record == null) {
        record = new TraderRecord(sub, 0, null);
        updateList.put(sub, record);
      }

      String pred = predicate.toString();
      // this is going to be bad
      String obj = object.toString();

      String simplePred = prefixMapper.simplifyWithNamespace(pred);
      // System.out.println("pred, simplePred:  " + pred + ", " + simplePred);

      if (simplePred.equals("link:TraderId")) {
        // String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        Object myObj = GraphUtils.extractTriple(triple);
        Integer iObj = (Integer)myObj;
        record.traderId = iObj.intValue();
      }
      if (simplePred.equals("link:Rating")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.rating = trimmed;
      }
      if (simplePred.equals("link:TradeHouse")) {
        // String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        Object myObj = GraphUtils.extractTriple(triple);
        Integer iObj = (Integer)myObj;
        record.traderId = iObj.intValue();
      }
      if (simplePred.equals("link:Name")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.name = trimmed;
      }
    }

    // System.out.println("total triples returned:  " + triples.size());

    return updateList;
  }


  /**
   * get a record
   */
  protected TraderRecord getRecord(int id)
    throws Exception {

    // get the wrapper
    if (wrapper == null) {
      connect();
    }

    StringBuilder sb = new StringBuilder();


    sb.append(prefixMapper.getPrefixesAsString());
    sb.append("\n");

    sb.append("\n");
    sb.append("CONSTRUCT {");
    sb.append("\n");
    sb.append("  ?sub link:TraderId ?traderId .");
    sb.append("\n");
    sb.append("  ?sub link:Rating ?rating .");
    sb.append("\n");
    sb.append("  ?sub link:TradeHouse ?tradeHouse .");
    sb.append("\n");
    sb.append("  ?sub link:Name ?name .");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");
    sb.append("WHERE");
    sb.append("\n");
    sb.append("{");
    sb.append("\n");
    sb.append("  GRAPH " + LINKAGE_GRAPH_NAME + " { " );
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_TRADER_ID_PREDICATE + " ?traderId .");
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_RATING_PREDICATE + " ?rating .");
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_TRADE_HOUSE_PREDICATE + " ?tradeHouse .");
    sb.append("\n");
    sb.append("    OPTIONAL { ?sub " + LINKAGE_NAME_PREDICATE + " ?name . } ");
    sb.append("\n");
    sb.append("    FILTER ( " + LINKAGE_TRADER_ID_PREDICATE + " = \"" + id + "\"^^xs:int ) ");
    sb.append("\n");
    sb.append("  }");
    sb.append("\n");

    sb.append("}");
    sb.append("\n");

    String query = sb.toString();

    // System.out.println(query);

    ArrayList<Triple> triples = GraphUtils.constructTriplesForQuery(wrapper, query);

    if (triples.size() == 0) {
      System.out.println("no triples found for Trader " + id);
      return null;
    }

    TraderRecord record = null;

    Iterator iter = triples.iterator();
    while (iter.hasNext()) {
      Triple triple = (Triple)iter.next();
      Node subject = triple.getSubject();
      Node predicate = triple.getPredicate();
      Node object = triple.getObject();
      String sub = subject.toString();

      // System.out.println("triple:  " + triple.toString());

      // blah blah
      if (record == null) {
        record = new TraderRecord(sub, 0, null);
      }

      String pred = predicate.toString();
      // this is going to be bad
      String obj = object.toString();

      String simplePred = prefixMapper.simplifyWithNamespace(pred);
      // System.out.println("pred, simplePred:  " + pred + ", " + simplePred);

      if (simplePred.equals("link:TraderId")) {
        // String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        Object myObj = GraphUtils.extractTriple(triple);
        Integer iObj = (Integer)myObj;
        record.traderId = iObj.intValue();
      }
      if (simplePred.equals("link:Rating")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.rating = trimmed;
      }
      if (simplePred.equals("link:TradeHouse")) {
        // String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        Object myObj = GraphUtils.extractTriple(triple);
        Integer iObj = (Integer)myObj;
        record.traderId = iObj.intValue();
      }
      if (simplePred.equals("link:Name")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.name = trimmed;
      }
    }

    // System.out.println("total triples returned:  " + triples.size());

    return record;
  }

  public TraderRecord getRandomRecord()
          throws Exception {

    HashMap<String, TraderRecord> records = null;

/*
    records = generateWorkload(500);
    int size = records.size();
    int pos = 0;
    if (size > 0)
      pos = Math.abs(randNum.nextInt() % records.size());
    int counter = 0;
    Iterator iter = records.values().iterator();
    while (iter.hasNext()) {
      TraderRecord record = (TraderRecord)iter.next();
      if (counter++ >= pos) {
        System.out.println("Trader getRandomRecord:  "
                              + records.size() + ", "
                              + pos + ", "
                              + record.traderId);
        return record;
      }
    }
*/
    records = generateWorkload(1);
    Iterator iter = records.values().iterator();
    while (iter.hasNext()) {
      TraderRecord record = (TraderRecord)iter.next();
      return record;
    }

    return null;
  }


}

