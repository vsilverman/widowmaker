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


public class TrackingDataManager {
  protected MarkLogicWrapper wrapper = null;

  protected static final boolean debugFlag = false;

  public static final String LINKAGE_GRAPH_NAME = "link:linkage";
  public static final String LINKAGE_SUBJECT_PREFIX = "link:Tracking_";
  public static final String LINKAGE_PRIMARY_KEY_PREDICATE = "link:PrimaryUri";
  public static final String LINKAGE_DOC_KEY_PREDICATE = "link:DocumentUri";
  public static final String LINKAGE_TEMPLATE_KEY_PREDICATE = "link:TemplateName";
  public static final String LINKAGE_CURRENT_STATUS_PREDICATE = "link:CurrentStatus";
  public static final String LINKAGE_SOURCE_ID_PREDICATE = "link:SourceId";

  protected String host;
  protected int port;
  protected String user;
  protected String password;
  protected String database;

  protected PrefixMapper prefixMapper;


  private static TrackingDataManager thisTracker = null;
  private boolean isInitialized = false;

  private TrackingDataManager() {
    prefixMapper = PrefixMapper.getPrefixMapper();
  }

  public static TrackingDataManager getTracker() {

    if (thisTracker == null) {
      thisTracker = new TrackingDataManager();
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
      System.out.println("TrackingDataManager:  no config file property present");
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
        System.out.println("TrackingDataManager:  no host entry in configuratiion");
      } else {
        Element hostElement = (Element)nlist.item(0);
        if (hostElement != null) {
          host = hostElement.getTextContent();
          System.out.println("TrackingDataManager:  host is " + host);
        } else {
          System.out.println("TrackingDataManager:  host is empty");
        }
      }

      nlist = rootElement.getElementsByTagName("port");
      if (nlist == null) {
        System.out.println("TrackingDataManager:  no port entry in configuratiion");
      } else {
        Element portElement = (Element)nlist.item(0);
        if (portElement != null) {
          port = Integer.parseInt(portElement.getTextContent());
          System.out.println("TrackingDataManager:  port is " + port);
        } else {
          System.out.println("TrackingDataManager:  port is empty");
        }
      }

      nlist = rootElement.getElementsByTagName("username");
      if (nlist == null) {
        System.out.println("TrackingDataManager:  no username entry in configuratiion");
      } else {
        Element usernameElement = (Element)nlist.item(0);
        if (usernameElement != null) {
          user = usernameElement.getTextContent();
          System.out.println("TrackingDataManager:  username is " + user);
        } else {
          System.out.println("TrackingDataManager:  username is empty");
        }
      }

      nlist = rootElement.getElementsByTagName("password");
      if (nlist == null) {
        System.out.println("TrackingDataManager:  no password entry in configuratiion");
      } else {
        Element passwordElement = (Element)nlist.item(0);
        if (passwordElement != null) {
          password = passwordElement.getTextContent();
          System.out.println("TrackingDataManager:  password is " + password);
        } else {
          System.out.println("TrackingDataManager:  password is empty");
        }
      }

      nlist = rootElement.getElementsByTagName("database");
      if (nlist == null) {
        System.out.println("TrackingDataManager:  no database entry in configuratiion");
      } else {
        Element databaseElement = (Element)nlist.item(0);
        if (databaseElement != null) {
          database = databaseElement.getTextContent();
          System.out.println("TrackingDataManager:  database is " + database);
        } else {
          System.out.println("TrackingDataManager:  database is empty");
        }
      }
    } catch (FileNotFoundException e) {
      System.out.println("TrackingDataManager:  file not found - " + filename);
    } catch (IOException e) {
      System.out.println("TrackingDataManager:  file error - " + filename);
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
    String sub = LINKAGE_SUBJECT_PREFIX + StressTester.randomString(8);

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


  protected UpdateStatusRecord storeTrackingData(String uri, String docUri, String template, String status) {

    // get the wrapper
    if (wrapper == null) {
      connect();
    }

    ArrayList<Triple> triples = new ArrayList<Triple>();

    // concoct a subject
    String sub = makeSubject();

    Node subject = NodeFactory.createURI(sub);
    Node predicate = NodeFactory.createURI(LINKAGE_PRIMARY_KEY_PREDICATE);
    Node object = NodeFactory.createLiteral(uri);

    Triple triple = Triple.create(subject, predicate, object);
    triples.add(triple);

    predicate = NodeFactory.createURI(LINKAGE_DOC_KEY_PREDICATE);
    object = NodeFactory.createLiteral(docUri);

    triple = Triple.create(subject, predicate, object);
    triples.add(triple);

    predicate = NodeFactory.createURI(LINKAGE_CURRENT_STATUS_PREDICATE);
    object = NodeFactory.createLiteral(status);

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
    sb.append(LINKAGE_PRIMARY_KEY_PREDICATE);
    sb.append(" \"");
    sb.append(uri);
    sb.append("\" . ");
    sb.append("\n");
    sb.append("    ");
    sb.append(subject);
    sb.append(" ");
    sb.append(LINKAGE_DOC_KEY_PREDICATE);
    sb.append(" \"");
    sb.append(docUri);
    sb.append("\" . ");
    sb.append("\n");
    sb.append("    ");
    sb.append(subject);
    sb.append(" ");
    sb.append(LINKAGE_TEMPLATE_KEY_PREDICATE);
    sb.append(" \"");
    sb.append(template);
    sb.append("\" . ");
    sb.append("\n");
    sb.append("    ");
    sb.append(subject);
    sb.append(" ");
    sb.append(LINKAGE_CURRENT_STATUS_PREDICATE);
    sb.append(" \"");
    sb.append(status);
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

    UpdateStatusRecord record = new UpdateStatusRecord();
    record.primaryUri = uri;
    record.docUri = docUri;
    record.currentStatus = status;

    return record;
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
    sb.append("link:Status link:total_uris ?total_uris .");
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
    sb.append("      ?s " + LINKAGE_PRIMARY_KEY_PREDICATE + " ?uri .");
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

      if (simplePred.equals("link:total_uris"))

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
  protected UpdateStatusRecord getOneRecord(String primaryUri)
    throws Exception {


    if (wrapper == null) {
      connect();
    }

    StringBuilder sb = new StringBuilder();


    sb.append(prefixMapper.getPrefixesAsString());
    sb.append("\n");

    sb.append("\n");
    sb.append("CONSTRUCT {");
    sb.append("\n");
    sb.append("  ?sub link:UpdateURI ?uri .");
    sb.append("\n");
    sb.append("  ?sub link:DocumentURI ?docuri .");
    sb.append("\n");
    sb.append("  ?sub link:CurrentStatus ?status .");
    sb.append("\n");
    sb.append("  ?sub " + LINKAGE_TEMPLATE_KEY_PREDICATE + " ?template .");
    sb.append("\n");
    sb.append("  ?sub link:SourceId ?srcId .");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");
    sb.append("WHERE");
    sb.append("\n");
    sb.append("{");
    sb.append("\n");
    sb.append("  GRAPH " + LINKAGE_GRAPH_NAME + " { " );
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_PRIMARY_KEY_PREDICATE + " ?uri .");
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_DOC_KEY_PREDICATE + " ?docuri .");
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_TEMPLATE_KEY_PREDICATE + " ?template .");
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_CURRENT_STATUS_PREDICATE + " ?status .");
    sb.append("\n");
    sb.append("    OPTIONAL { ?sub " + LINKAGE_SOURCE_ID_PREDICATE + " ?srcId . } ");
    sb.append("\n");
    sb.append("    FILTER ( ?uri = \"" + primaryUri + "\" ) ");
    sb.append("\n");
    sb.append("  }");
    sb.append("\n");

    sb.append("}");
    sb.append("\n");

    String query = sb.toString();

    if (debugFlag) {
      System.out.println(query);
    }

    UpdateStatusRecord record = null;
    ArrayList<Triple> triples = GraphUtils.constructTriplesForQuery(wrapper, query);
    Iterator iter = triples.iterator();
    while (iter.hasNext()) {
      Triple triple = (Triple)iter.next();
      Node subject = triple.getSubject();
      Node predicate = triple.getPredicate();
      Node object = triple.getObject();
      String sub = subject.toString();

      if (record == null) {
        record = new UpdateStatusRecord(sub, null, null, (String)null);
      }
      // System.out.println("triple:  " + triple.toString());

      String pred = predicate.toString();
      // this is going to be bad
      String obj = object.toString();

      String simplePred = prefixMapper.simplifyWithNamespace(pred);
      // System.out.println("pred, simplePred:  " + pred + ", " + simplePred);

      if (simplePred.equals("link:UpdateURI")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.primaryUri = trimmed;
      }
      if (simplePred.equals("link:DocumentURI")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.docUri = trimmed;
      }
      if (simplePred.equals("link:TemplateName")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.templateName = trimmed;
      }
      if (simplePred.equals("link:CurrentStatus")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.currentStatus = trimmed;
      }
      if (simplePred.equals("link:SourceId")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.srcId = Integer.parseInt(trimmed);
      }
    }

    // System.out.println("total triples returned:  " + triples.size());

    return record;
  }


  /**
   * decide which documents to update
   */
  protected HashMap<String, UpdateStatusRecord> generateWorkload(int numRecords)
    throws Exception {


    HashMap<String, UpdateStatusRecord> updateList = new HashMap<String, UpdateStatusRecord>();

    int limit = numRecords;
    // temporary for debugging
    // limit = 10;

    // System.out.println("generateWorkload:  limit is set to " + limit);

    // get the wrapper
    if (wrapper == null) {
      connect();
    }

    StringBuilder sb = new StringBuilder();


/*
prefix link: <http://marklogic.com/stress/tde/linkage/dbts#> 
prefix xs: <http://www.w3.org/2001/XMLSchema#> 


construct {
  ?sub link:UpdateURI ?uri .
  ?sub link:CurrentStatus ?status .
}
where {
  graph link:linkage {
    ?sub link:PrimaryUri ?uri .
    ?sub link:CurrentStatus ?status .
  }
  {
    select ?sub
    where
    {
      graph link:linkage {
        ?sub link:PrimaryUri ?thisuri .
      }
    }
    order by rand()
    limit 10
  }
}
*/

    sb.append(prefixMapper.getPrefixesAsString());
    sb.append("\n");

    sb.append("\n");
    sb.append("CONSTRUCT {");
    sb.append("\n");
    sb.append("  ?sub link:UpdateURI ?uri .");
    sb.append("\n");
    sb.append("  ?sub link:DocumentURI ?docuri .");
    sb.append("\n");
    sb.append("  ?sub " + LINKAGE_TEMPLATE_KEY_PREDICATE + " ?template .");
    sb.append("\n");
    sb.append("  ?sub link:CurrentStatus ?status .");
    sb.append("\n");
    sb.append("  ?sub link:SourceId ?srcId .");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");
    sb.append("WHERE");
    sb.append("\n");
    sb.append("{");
    sb.append("\n");
    sb.append("  GRAPH " + LINKAGE_GRAPH_NAME + " { " );
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_PRIMARY_KEY_PREDICATE + " ?uri .");
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_DOC_KEY_PREDICATE + " ?docuri .");
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_TEMPLATE_KEY_PREDICATE + " ?template .");
    sb.append("\n");
    sb.append("    ?sub " + LINKAGE_CURRENT_STATUS_PREDICATE + " ?status .");
    sb.append("\n");
    sb.append("    OPTIONAL { ?sub " + LINKAGE_SOURCE_ID_PREDICATE + " ?srcId . } ");
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
    sb.append("      graph link:linkage {");
    sb.append("\n");
    sb.append("        ?sub link:PrimaryUri ?thisuri .");
    sb.append("\n");
    sb.append("        ?sub link:CurrentStatus ?thisstatus .");
    sb.append("\n");
    sb.append("        filter(?thisstatus != \"SETTLED\")");
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

    if (debugFlag) {
      System.out.println(query);
    }

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
      UpdateStatusRecord record = (UpdateStatusRecord)updateList.get(sub);
      if (record == null) {
        record = new UpdateStatusRecord(sub, null, null, (String)null);
        updateList.put(sub, record);
      }

      String pred = predicate.toString();
      // this is going to be bad
      String obj = object.toString();

      String simplePred = prefixMapper.simplifyWithNamespace(pred);
      // System.out.println("pred, simplePred:  " + pred + ", " + simplePred);

      if (simplePred.equals("link:UpdateURI")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.primaryUri = trimmed;
      }
      if (simplePred.equals("link:DocumentURI")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.docUri = trimmed;
      }
      if (simplePred.equals("link:TemplateName")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.templateName = trimmed;
      }
      if (simplePred.equals("link:CurrentStatus")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.currentStatus = trimmed;
      }
      if (simplePred.equals("link:SourceId")) {
        String trimmed = GraphUtils.trimSurroundingQuotes(obj);
        record.srcId = Integer.parseInt(trimmed);
      }
    }

    // System.out.println("total triples returned:  " + triples.size());

    return updateList;
  }

  public String updateStatusData(UpdateStatusRecord record, String newStatus) {

    if ((record == null) || (newStatus == null)) {
      System.out.println("updateStatusData:  null data supplied");
      return null;
    }

    // get the wrapper
    if (wrapper == null) {
      connect();
    }

    String enclosedNewStatus = "\"" + newStatus + "\"";

    StringBuffer sb = new StringBuffer();

    sb.append(prefixMapper.getPrefixesAsString());
    sb.append("\n");
    sb.append("\n");
    
    sb.append("WITH link:linkage");
    sb.append("\n");
    sb.append("DELETE {");
    sb.append("\n");
    sb.append("  ?sub link:CurrentStatus ?status .");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");
    sb.append("INSERT {");
    sb.append("\n");
    sb.append("  ?sub link:CurrentStatus " + enclosedNewStatus + " . ");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");
    sb.append("WHERE {");
    sb.append("\n");
    sb.append("  ?sub link:CurrentStatus ?status .");
    sb.append("\n");
    sb.append("  FILTER (?sub = " + prefixMapper.simplifyWithNamespace(record.sub) + ")");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");

    String query = sb.toString();

    GraphUtils.updateTripleQuery(wrapper, query);

    return newStatus;
  }

  public String updateStatusData(UpdateStatusRecord record, UpdateStatusRecord newRecord) {

    if ((record == null) || (newRecord == null)) {
      System.out.println("updateStatusData:  null data supplied");
      return null;
    }

    // get the wrapper
    if (wrapper == null) {
      connect();
    }

    String enclosedNewStatus = "\"" + newRecord.currentStatus + "\"";
    String enclosedNewSourceId = "\"" + newRecord.srcId + "\"^^xs:integer";
    StringBuffer sb = null;
    String query = null;

    if (!record.currentStatus.equals(newRecord.currentStatus)) {
    sb = new StringBuffer();

    sb.append(prefixMapper.getPrefixesAsString());
    sb.append("\n");
    sb.append("\n");
    
    sb.append("WITH link:linkage");
    sb.append("\n");
    sb.append("DELETE {");
    sb.append("\n");
    sb.append("  ?sub link:CurrentStatus ?status .");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");
    sb.append("INSERT {");
    sb.append("\n");
    sb.append("  ?sub link:CurrentStatus " + enclosedNewStatus + " . ");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");
    sb.append("WHERE {");
    sb.append("\n");
    sb.append("  ?sub link:CurrentStatus ?status .");
    sb.append("\n");
    sb.append("  FILTER (?sub = " + prefixMapper.simplifyWithNamespace(record.sub) + ")");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");

    query = sb.toString();

    GraphUtils.updateTripleQuery(wrapper, query);

    }

    if (record.srcId != newRecord.srcId) {
    sb = new StringBuffer();

    sb.append(prefixMapper.getPrefixesAsString());
    sb.append("\n");
    sb.append("\n");
    
    sb.append("WITH link:linkage");
    sb.append("\n");
    sb.append("DELETE {");
    sb.append("\n");
    sb.append("  ?sub link:SourceId ?id .");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");
    sb.append("INSERT {");
    sb.append("\n");
    sb.append("  ?sub link:SourceId " + enclosedNewSourceId + " . ");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");
    sb.append("WHERE {");
    sb.append("\n");
    sb.append("  ?sub link:SourceId ?id .");
    sb.append("\n");
    sb.append("  FILTER (?sub = " + prefixMapper.simplifyWithNamespace(record.sub) + ")");
    sb.append("\n");
    sb.append("}");
    sb.append("\n");

    query = sb.toString();

    GraphUtils.updateTripleQuery(wrapper, query);

    }

    return null;
  }

  public void updateStatusData(HashMap<String, UpdateStatusRecord> updateList) {

    if (updateList == null) {
      System.out.println("updateStatusData:  null data supplied");
      return;
    }

    // get the wrapper
    if (wrapper == null) {
      connect();
    }

    StringBuffer sb = new StringBuffer();

    sb.append(prefixMapper.getPrefixesAsString());
    sb.append("\n");
    sb.append("\n");
    
    sb.append("WITH link:linkage");
    sb.append("\n");
    sb.append("DELETE {");
    sb.append("\n");


    Iterator iter = updateList.values().iterator();
    while (iter.hasNext()) {

      UpdateStatusRecord record = (UpdateStatusRecord)iter.next();
      String enclosedCurrentStatus = "\"" + record.currentStatus + "\"";

      sb.append("  ");
      sb.append(prefixMapper.simplifyWithNamespace(record.sub));
      sb.append(" ");
      sb.append(LINKAGE_CURRENT_STATUS_PREDICATE);
      sb.append(" ");
      sb.append(enclosedCurrentStatus);
      sb.append(" .");
      sb.append("\n");

    // sb.append("  ?sub link:CurrentStatus ?status .");
    // sb.append("\n");

    }

    sb.append("}");
    sb.append("\n");

    sb.append("INSERT {");
    sb.append("\n");

    iter = updateList.values().iterator();
    while (iter.hasNext()) {

      // sb.append("  ?sub link:CurrentStatus " + enclosedNewStatus + " . ");
      // sb.append("\n");


      UpdateStatusRecord record = (UpdateStatusRecord)iter.next();
      String enclosedNewStatus = "\"" + record.newStatus + "\"";

      sb.append("  ");
      sb.append(prefixMapper.simplifyWithNamespace(record.sub));
      sb.append(" ");
      sb.append(LINKAGE_CURRENT_STATUS_PREDICATE);
      sb.append(" ");
      sb.append(enclosedNewStatus);
      sb.append(" .");
      sb.append("\n");


    }

    sb.append("}");
    sb.append("\n");

    sb.append("WHERE { ");
    // sb.append("\n");
    // sb.append("  ?sub link:CurrentStatus ?status .");
    // sb.append("\n");
    // sb.append("  filter (?sub = " + prefixMapper.simplifyWithNamespace(record.sub) + ")");
    // sb.append("\n");

    sb.append("}");
    sb.append("\n");

    String query = sb.toString();

    if (debugFlag) {
      System.out.println(query);
    }

    GraphUtils.updateTripleQuery(wrapper, query);

  }

}

