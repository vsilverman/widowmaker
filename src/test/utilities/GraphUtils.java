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
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Collections;
import java.util.StringTokenizer;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicDatasetGraphFactory;
import com.marklogic.semantics.jena.MarkLogicJenaException;

// for sparql update
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateProcessor;
import com.hp.hpl.jena.update.UpdateRequest;

import org.joda.time.DateTime;
import org.joda.time.LocalTime;
import org.joda.time.Period;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.shared.PrefixMapping;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDateTime;
import com.hp.hpl.jena.datatypes.xsd.XSDDuration;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.Quad;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.graph.Triple;


/**
 * eventually this will be a static class
 */

public class GraphUtils {

    protected static final String PREFIX_PROPERTY_PREFIX = "prefixes.";

    private static boolean debugMode = false;
    private static boolean verboseDebugMode = false;

    // this is temporary until prefixes move
    // protected String queueDirectory;

    public GraphUtils() {
    }

    public static void setDebugMode(boolean val) {
        debugMode = val;
    }

    public static void setVerboseDebugMode(boolean val) {
        verboseDebugMode = val;
    }

    public void handleCmd(String cmd, String line, PrintWriter out) {
    }

/*
    private void makeLiteral() {
        Model model = ModelFactory.createDefaultModel();


        Resource subject = r("s");
        
        model.addLiteral (subject, p("p1"), 10);
        model.addLiteral (subject, p("p2"), 0.5);
        model.addLiteral (subject, p("p3"), (float)0.5);
        model.addLiteral (subject, p("p4"), l(20));
        model.addLiteral (subject, p("p5"), l(0.99));
        model.addLiteral (subject, p("p6"), true);
        model.add (subject, p("p7"), l("2012-03-11", XSDDatatype.XSDdate));
        model.add (subject, p("p8"), l("P2Y", XSDDatatype.XSDduration));

        model.setNsPrefix("example", BASE);
        model.setNsPrefix("xsd", "http://www.w3.org/2001/XMLSchema#");

        model.write(System.out, "TURTLE");
    }
    
    private static Resource r ( String localname ) {
        return ResourceFactory.createResource ( BASE + localname );
    }
    
    private static Property p ( String localname ) {
        return ResourceFactory.createProperty ( BASE, localname );
    }

    private static Literal l ( Object value ) {
        return ResourceFactory.createTypedLiteral ( value );
    }

    private static Literal l ( String lexicalform, RDFDatatype datatype ) {
        return ResourceFactory.createTypedLiteral ( lexicalform, datatype );
    }
*/

    protected static void verboseDebug(PrintStream os, String str) {
      if (verboseDebugMode)
        os.println(str);
    }

    protected static ArrayList<Triple> getTriples(String graph, String subject) {

        ArrayList<Triple> triples = new ArrayList<Triple>();


        return triples;
    }

    /**
     * TODO:  move this to ModelUtils
     * TODO:  make a version that converts it to an Array of Strings
     */
    protected String convertModelToTriplesString(Model m) {
        return convertModelToTriplesString(m, false);
    }

    /**
     * TODO:  work in progress decoding a triple
     */
    protected static void decodeTriple(Triple triple) {

      if (triple == null)
        return;

      // left in here just to keep the code consistent
      StringBuilder sb = new StringBuilder();
      boolean usePrefixes = true;

      Node subject = triple.getSubject();
      Node predicate = triple.getPredicate();
      Node obj = triple.getObject();
      RDFDatatype datatype;
      String typeURI = "";

      verboseDebug(System.out, "subject:  " + subject.toString());
      verboseDebug(System.out, "predicate:  " + predicate.toString());

      if (usePrefixes)
          sb.append(simplifyWithNamespace(subject.toString()));
      else
          sb.append(subject.toString());
      sb.append(" ");
      if (usePrefixes)
          sb.append(simplifyWithNamespace(predicate.toString()));
      else
          sb.append(predicate.toString());
      sb.append(" ");

      if (obj.isLiteral()) {
          // Literal literal = obj.getLiteral();
          datatype = obj.getLiteralDatatype();
          if (datatype == null) {
              // String str = literal.getString();
              String str = obj.toString();
              verboseDebug(System.out, "is plain literal:");
              verboseDebug(System.out, "    str is " + str);
              sb.append("\"");
              sb.append(str);
              sb.append("\"");
              sb.append(" . \n");
          } else {
              String dUri = datatype.getURI();
              // String str = literal.getString();
              String str = obj.toString();
              Object o = obj.getLiteralValue();
              verboseDebug(System.out, "is a typed literal:");
              verboseDebug(System.out, "    dUri is " + dUri);
              verboseDebug(System.out, "    data is " + str);
              verboseDebug(System.out, "    dataTypeURI is " + obj.getLiteralDatatypeURI());
              verboseDebug(System.out, "    lexical form is " + obj.getLiteralLexicalForm());
              verboseDebug(System.out, "    object type:  " + o.getClass().getName());
              Class c = datatype.getJavaClass();
              if (c != null) {
                verboseDebug(System.out, "    java class:  " + datatype.getJavaClass().getName());
              } else {
                verboseDebug(System.out, "    appears to be classless");
              }
              if (o instanceof Integer) {
                // int v = literal.getInt();
                int v = ((Integer)o).intValue();
                verboseDebug(System.out, "    int:  " + v);
              }
              else if (o instanceof Long) {
                long v = ((Long)o).longValue();
                verboseDebug(System.out, "    long:  " + v);
              }
              else if (o instanceof Double) {
                double v = ((Double)o).doubleValue();
                verboseDebug(System.out, "    double:  " + v);
              }
              else if (o instanceof Float) {
                float v = ((Float)o).floatValue();
                verboseDebug(System.out, "    float:  " + v);
              }
              else if (o instanceof Boolean) {
                boolean v = ((Boolean)o).booleanValue();
                verboseDebug(System.out, "    boolean:  " + v);
              }
              else if (o instanceof Short) {
                short v = ((Short)o).shortValue();
                verboseDebug(System.out, "    short:  " + v);
              }
              else if (o instanceof XSDDateTime) {
                String v = obj.toString();
                verboseDebug(System.out, "    dateTime:  " + v);
                // figure out if this is a date or a time
                if (datatype == XSDDatatype.XSDdateTime) {
                v = obj.getLiteralLexicalForm();
                DateTime dtime = new DateTime(v);
                verboseDebug(System.out, "    dateTime obj:  " + dtime.toString());
                }
                if (datatype == XSDDatatype.XSDdate) {
                v = obj.getLiteralLexicalForm();
                DateTime dtime = new DateTime(v);
                verboseDebug(System.out, "    dateTime obj:  " + dtime.toString());
                }
                if (datatype == XSDDatatype.XSDtime) {
                v = obj.getLiteralLexicalForm();
                LocalTime ltime = LocalTime.parse(v);
                verboseDebug(System.out, "    LocalTime obj:  " + ltime.toString());
                }
              }
              else if (o instanceof XSDDuration) {
                String v = obj.toString();
                v = obj.getLiteralLexicalForm();
                Period period = Period.parse(v);
                verboseDebug(System.out, "    period duration:  " + period);
              } else {
                String v = obj.toString();
                verboseDebug(System.out, "    HUH? somehow we missed" + v);
              }

              sb.append("\"");
              sb.append(str);
              sb.append("\"^^");
              if (usePrefixes)
                  sb.append(simplifyWithNamespace(dUri));
              else
                  sb.append(dUri);
              sb.append(" .\n");

          }
      } else {
          typeURI = obj.toString();
          verboseDebug(System.out, "not a literal:");
          verboseDebug(System.out, "    typeURI = " + typeURI);
          verboseDebug(System.out, "    simplified = " + simplifyWithNamespace(typeURI));
          sb.append(simplifyWithNamespace(typeURI));
          sb.append(" .\n");
      }


    }

    /**
     * TODO:  work in progress decoding a triple
     */
    public static Object extractTriple(Triple triple) {

      Object rObj = null;

      if (triple == null)
        return rObj;

      // left in here just to keep the code consistent
      StringBuilder sb = new StringBuilder();
      boolean usePrefixes = true;

      Node subject = triple.getSubject();
      Node predicate = triple.getPredicate();
      Node obj = triple.getObject();
      RDFDatatype datatype;
      String typeURI = "";

      verboseDebug(System.out, "subject:  " + subject.toString());
      verboseDebug(System.out, "predicate:  " + predicate.toString());

      // if (usePrefixes)
          // sb.append(simplifyWithNamespace(subject.toString()));
      // else
          // sb.append(subject.toString());
      // sb.append(" ");
      // if (usePrefixes)
          // sb.append(simplifyWithNamespace(predicate.toString()));
      // else
          // sb.append(predicate.toString());
      // sb.append(" ");

      if (obj.isLiteral()) {
          // Literal literal = obj.getLiteral();
          datatype = obj.getLiteralDatatype();
          if (datatype == null) {
              // String str = literal.getString();
              String str = obj.toString();
              rObj = str;
              // verboseDebug(System.out, "is plain literal:");
              // verboseDebug(System.out, "    str is " + str);
              // sb.append("\"");
              // sb.append(str);
              // sb.append("\"");
              // sb.append(" . \n");
          } else {
              String dUri = datatype.getURI();
              // String str = literal.getString();
              String str = obj.toString();
              Object o = obj.getLiteralValue();
              rObj = o;
              // verboseDebug(System.out, "is a typed literal:");
              // verboseDebug(System.out, "    dUri is " + dUri);
              // verboseDebug(System.out, "    data is " + str);
              // verboseDebug(System.out, "    dataTypeURI is " + obj.getLiteralDatatypeURI());
              // verboseDebug(System.out, "    lexical form is " + obj.getLiteralLexicalForm());
              // verboseDebug(System.out, "    object type:  " + o.getClass().getName());
              Class c = datatype.getJavaClass();
              if (c != null) {
                // verboseDebug(System.out, "    java class:  " + datatype.getJavaClass().getName());
              } else {
                // verboseDebug(System.out, "    appears to be classless");
              }
              if (o instanceof Integer) {
                // int v = literal.getInt();
                int v = ((Integer)o).intValue();
                // verboseDebug(System.out, "    int:  " + v);
              }
              else if (o instanceof Long) {
                long v = ((Long)o).longValue();
                // verboseDebug(System.out, "    long:  " + v);
              }
              else if (o instanceof Double) {
                double v = ((Double)o).doubleValue();
                // verboseDebug(System.out, "    double:  " + v);
              }
              else if (o instanceof Float) {
                float v = ((Float)o).floatValue();
                // verboseDebug(System.out, "    float:  " + v);
              }
              else if (o instanceof Boolean) {
                boolean v = ((Boolean)o).booleanValue();
                // verboseDebug(System.out, "    boolean:  " + v);
              }
              else if (o instanceof Short) {
                short v = ((Short)o).shortValue();
                // verboseDebug(System.out, "    short:  " + v);
              }
              else if (o instanceof XSDDateTime) {
                String v = obj.toString();
                // verboseDebug(System.out, "    dateTime:  " + v);
                // figure out if this is a date or a time
                if (datatype == XSDDatatype.XSDdateTime) {
                v = obj.getLiteralLexicalForm();
                DateTime dtime = new DateTime(v);
                rObj = dtime;
                // verboseDebug(System.out, "    dateTime obj:  " + dtime.toString());
                }
                if (datatype == XSDDatatype.XSDdate) {
                v = obj.getLiteralLexicalForm();
                DateTime dtime = new DateTime(v);
                rObj = dtime;
                // verboseDebug(System.out, "    dateTime obj:  " + dtime.toString());
                }
                if (datatype == XSDDatatype.XSDtime) {
                v = obj.getLiteralLexicalForm();
                LocalTime ltime = LocalTime.parse(v);
                rObj = ltime;
                // verboseDebug(System.out, "    LocalTime obj:  " + ltime.toString());
                }
              }
              else if (o instanceof XSDDuration) {
                String v = obj.toString();
                v = obj.getLiteralLexicalForm();
                Period period = Period.parse(v);
                rObj = period;
                // verboseDebug(System.out, "    period duration:  " + period);
              } else {
                String v = obj.toString();
                rObj = v;
                // verboseDebug(System.out, "    HUH? somehow we missed" + v);
              }

              // sb.append("\"");
              // sb.append(str);
              // sb.append("\"^^");
              // if (usePrefixes)
                  // sb.append(simplifyWithNamespace(dUri));
              // else
                  // sb.append(dUri);
              // sb.append(" .\n");

          }
      } else {
          typeURI = obj.toString();
          rObj = obj;
          // verboseDebug(System.out, "not a literal:");
          // verboseDebug(System.out, "    typeURI = " + typeURI);
          // verboseDebug(System.out, "    simplified = " + simplifyWithNamespace(typeURI));
          // sb.append(simplifyWithNamespace(typeURI));
          // sb.append(" .\n");
      }

      return rObj;
    }

    /**
     * TODO:  move this to ModelUtils
     * TODO:  make a version that converts it to an Array of Strings
     */
    protected static String convertModelToTriplesString(Model m, boolean usePrefixes) {

        System.out.println("GraphUtils.convertModelToTriplesString");

        StringBuilder sb = new StringBuilder();

        StmtIterator stmtIterator = m.listStatements();
        while (stmtIterator.hasNext()) {
            Statement statement = stmtIterator.nextStatement();
            Resource subject = statement.getSubject();
            Property predicate = statement.getPredicate();
            RDFNode obj = statement.getObject();
            RDFDatatype datatype;
            String typeURI = "";

            verboseDebug(System.out, "subject:  " + subject.toString());
            verboseDebug(System.out, "predicate:  " + predicate.toString());

            if (usePrefixes)
                sb.append(simplifyWithNamespace(subject.toString()));
            else
                sb.append(subject.toString());
            sb.append(" ");
            if (usePrefixes)
                sb.append(simplifyWithNamespace(predicate.toString()));
            else
                sb.append(predicate.toString());
            sb.append(" ");

            if (obj.isLiteral()) {
                Literal literal = obj.asLiteral();
                datatype = literal.getDatatype();
                if (datatype == null) {
                    String str = literal.getString();
                    verboseDebug(System.out, "is plain literal:");
                    verboseDebug(System.out, "    str is " + str);
                    sb.append("\"");
                    sb.append(str);
                    sb.append("\"");
                    sb.append(" . \n");
                } else {
                    String dUri = datatype.getURI();
                    String str = literal.getString();
                    Object o = literal.getValue();
                    verboseDebug(System.out, "is a typed literal:");
                    verboseDebug(System.out, "    dUri is " + dUri);
                    verboseDebug(System.out, "    data is " + str);
                    verboseDebug(System.out, "    dataTypeURI is " + literal.getDatatypeURI());
                    verboseDebug(System.out, "    lexical form is " + literal.getLexicalForm());
                    verboseDebug(System.out, "    literal as string:  " + literal.toString());
                    verboseDebug(System.out, "    object type:  " + o.getClass().getName());
                    Class c = datatype.getJavaClass();
                    if (c != null) {
                      verboseDebug(System.out, "    java class:  " + datatype.getJavaClass().getName());
                    } else {
                      verboseDebug(System.out, "    appears to be classless");
                    }
                    if (o instanceof Integer) {
                      int v = literal.getInt();
                      verboseDebug(System.out, "    int:  " + v);
                    }
                    else if (o instanceof Long) {
                      long v = literal.getLong();
                      verboseDebug(System.out, "    long:  " + v);
                    }
                    else if (o instanceof Double) {
                      double v = literal.getDouble();
                      verboseDebug(System.out, "    double:  " + v);
                    }
                    else if (o instanceof Float) {
                      float v = literal.getFloat();
                      verboseDebug(System.out, "    float:  " + v);
                    }
                    else if (o instanceof Boolean) {
                      boolean v = literal.getBoolean();
                      verboseDebug(System.out, "    boolean:  " + v);
                    }
                    else if (o instanceof Short) {
                      short v = literal.getShort();
                      verboseDebug(System.out, "    short:  " + v);
                    }
                    else if (o instanceof XSDDateTime) {
                      String v = literal.getString();
                      verboseDebug(System.out, "    dateTime:  " + v);
                      // figure out if this is a date or a time
                      if (datatype == XSDDatatype.XSDdateTime) {
                      DateTime dtime = new DateTime(v);
                      verboseDebug(System.out, "    dateTime obj:  " + dtime.toString());
                      }
                      if (datatype == XSDDatatype.XSDdate) {
                      DateTime dtime = new DateTime(v);
                      verboseDebug(System.out, "    dateTime obj:  " + dtime.toString());
                      }
                      if (datatype == XSDDatatype.XSDtime) {
                      LocalTime ltime = LocalTime.parse(v);
                      verboseDebug(System.out, "    LocalTime obj:  " + ltime.toString());
                      }
                    }
                    else if (o instanceof XSDDuration) {
                      String v = literal.getString();
                      Period period = Period.parse(v);
                      verboseDebug(System.out, "    period duration:  " + period);
                    } else {
                      String v = literal.getString();
                      verboseDebug(System.out, "    HUH? somehow we missed" + v);
                    }

                    sb.append("\"");
                    sb.append(str);
                    sb.append("\"^^");
                    if (usePrefixes)
                        sb.append(simplifyWithNamespace(dUri));
                    else
                        sb.append(dUri);
                    sb.append(" .\n");

                }
            } else {
                typeURI = obj.toString();
                verboseDebug(System.out, "not a literal:");
                verboseDebug(System.out, "    typeURI = " + typeURI);
                verboseDebug(System.out, "    simplified = " + simplifyWithNamespace(typeURI));
                sb.append(simplifyWithNamespace(typeURI));
                sb.append(" .\n");
            }
        }

        verboseDebug(System.out, "convertModelToTriplesString:");
        verboseDebug(System.out, sb.toString());

        return sb.toString();

    }

    /**
     * TODO:  move this to ModelUtils
     * TODO:  make a version that converts it to an Array of Strings
     */
    protected static ArrayList<String> convertModelToStringArray(Model m) {
        return convertModelToStringArray(m, false);
    }

    /**
     * TODO:  move this to ModelUtils
     * TODO:  make a version that converts it to an Array of Strings
     */
    protected static ArrayList<String> convertModelToStringArray(Model m, boolean usePrefixes) {

        ArrayList<String> strings = new ArrayList<String>();

        StmtIterator stmtIterator = m.listStatements();
        while (stmtIterator.hasNext()) {
            Statement statement = stmtIterator.nextStatement();
            Resource subject = statement.getSubject();
            Property predicate = statement.getPredicate();
            RDFNode obj = statement.getObject();
            RDFDatatype datatype;
            String typeURI = "";

            StringBuilder sb = new StringBuilder();

            // System.out.println("subject:  " + subject.toString());
            // System.out.println("predicate:  " + predicate.toString());

            if (usePrefixes)
                sb.append(simplifyWithNamespace(subject.toString()));
            else
                sb.append(subject.toString());
            sb.append(" ");
            if (usePrefixes)
                sb.append(simplifyWithNamespace(predicate.toString()));
            else
                sb.append(predicate.toString());
            sb.append(" ");

            if (obj.isLiteral()) {
                Literal literal = obj.asLiteral();
                datatype = literal.getDatatype();
                if (datatype == null) {
                    String str = literal.getString();
                    // System.out.println("is plain literal:  str is " + str);
                    sb.append("\"");
                    sb.append(str);
                    sb.append("\"");
                    sb.append(" . \n");
                } else {
                    String dUri = datatype.getURI();
                    String str = literal.getString();
                    // System.out.println("is a typed literal:  dUri is " + dUri);
                    // System.out.println("    data is " + str);
                    sb.append("\"");
                    sb.append(str);
                    sb.append("\"^^");
                    if (usePrefixes)
                        sb.append(simplifyWithNamespace(dUri));
                    else
                        sb.append(dUri);
                    sb.append(" .\n");

                }
            } else {
                typeURI = obj.toString();
                // System.out.println("ERROR   --- ERROR");
                // System.out.println("not a literal:  typeURI = " + typeURI);
                sb.append(simplifyWithNamespace(typeURI));
                sb.append(" .\n");
            }

            strings.add(sb.toString());

        }

        return strings;

    }

    /**
     * TODO:  move this to ModelUtils
     * TODO:  make a version that converts it to an Array of Strings
     */
    protected static ArrayList<Triple> convertModelToTriplesArray(Model m, boolean usePrefixes) {

        ArrayList<Triple> triples = new ArrayList<Triple>();
        Triple triple = null;

        StmtIterator stmtIterator = m.listStatements();
        while (stmtIterator.hasNext()) {
            Statement statement = stmtIterator.nextStatement();
            Resource subject = statement.getSubject();
            Property predicate = statement.getPredicate();
            RDFNode obj = statement.getObject();
            RDFDatatype datatype;
            String typeURI = "";

            triple = statement.asTriple();

/*
            StringBuilder sb = new StringBuilder();

            // System.out.println("subject:  " + subject.toString());
            // System.out.println("predicate:  " + predicate.toString());

            if (usePrefixes)
                sb.append(simplifyWithNamespace(subject.toString()));
            else
                sb.append(subject.toString());
            sb.append(" ");
            if (usePrefixes)
                sb.append(simplifyWithNamespace(predicate.toString()));
            else
                sb.append(predicate.toString());
            sb.append(" ");

            if (obj.isLiteral()) {
                Literal literal = obj.asLiteral();
                datatype = literal.getDatatype();
                if (datatype == null) {
                    String str = literal.getString();
                    // System.out.println("is plain literal:  str is " + str);
                    sb.append("\"");
                    sb.append(str);
                    sb.append("\"");
                    sb.append(" . \n");
                } else {
                    String dUri = datatype.getURI();
                    String str = literal.getString();
                    // System.out.println("is a typed literal:  dUri is " + dUri);
                    // System.out.println("    data is " + str);
                    sb.append("\"");
                    sb.append(str);
                    sb.append("\"^^");
                    if (usePrefixes)
                        sb.append(simplifyWithNamespace(dUri));
                    else
                        sb.append(dUri);
                    sb.append(" .\n");

                }
            } else {
                typeURI = obj.toString();
                // System.out.println("ERROR   --- ERROR");
                // System.out.println("not a literal:  typeURI = " + typeURI);
                sb.append(simplifyWithNamespace(typeURI));
                sb.append(" .\n");
            }

            // strings.add(sb.toString());

            // System.out.println("as string:  " + sb.toString());
*/

            // Triple triple = Triple.create(subject, predicate, obj);
            triples.add(triple);

            // System.out.println("as triple:  " + triple.toString());
        }

        return triples;

    }

    /**
     * @g   named graph
     * @s   subject - all existing triples for this subject will be deleted
     * @triples - array of new triples to insert into the graph
     */
    public static void  displayTriplesForSubject(MarkLogicWrapper wrapper, String g, String s) {


      System.out.println("GraphUtils.displayTriplesForSubject");
/*
        try {
            // this is now passed in
            // wrapper = getDatasetGraph();
            
            String prefixStr = getPrefixesAsString();
            StringBuilder sb = new StringBuilder();
            sb.append(prefixStr);
            sb.append(" ");

            sb.append("with " + g + " ");
            sb.append( " ");
            sb.append( "insert {");
            sb.append( " " );

            Iterator iter = triples.iterator();
            while (iter.hasNext()) {
                Triple triple = (Triple)iter.next();
                sb.append(triple.getSubject().getLiteral());
                sb.append(" ");
                sb.append(triple.getPredicate().getLiteral());
                sb.append(" ");

                // RDFDatatype datatype = triple.getObject().asLiteral().getDatatype();
                Node obj = triple.getObject();
                if (obj.isLiteral()) {
                    RDFDatatype datatype = obj.getLiteralDatatype();
                    Object o = obj.getLiteralValue();
                } else {
                    String uri = obj.getURI();
                    sb.append(uri);
                    sb.append(" ");
                }

                // just in case for now
                sb.append(" ");

            }
            // sb.append( "  wthr:today wthr:DummySeedKey \"2015-11-19T15:35:00\"^^xs:dateTime . ");

            sb.append( " ");
            sb.append( "} " );
            sb.append( " where {} " );

            String insertData = sb.toString();

            update = UpdateFactory.create(insertData);
            processor = UpdateExecutionFactory.create(update, wrapper.getDatasetGraph());
            processor.execute();

            // RDFDataMgr.write(System.out, wrapper.getGraph(NodeFactory.createURI("http://jameshillfarm.com/weather#yearly")), RDFFormat.TURTLE);


        } catch (Exception e) {
            e.printStackTrace();
        }

*/

        try {
            // this is now passed in
            // wrapper = getDatasetGraph();

            String query = getPrefixesAsString() 
                + " "
                + " construct { ?s ?p ?o . } "
                + " where { "
                + (g != null ? ("   graph " + g + " { ") : "" )
                + "     ?s ?p ?o . "
                + "     filter (?s = " + s + " ) "
                + (g != null ? ("   } ") : "" )
                + " } " ;

            System.out.println(query);

            QueryExecution execution = QueryExecutionFactory.create(
                    query, wrapper.getDatasetGraph().toDataset());

            // System.out.println("calling execConstruct");
            Model model = execution.execConstruct();
            // System.out.println("finished calling execConstruct");


            // RDFDataMgr.write(System.out, model, RDFFormat.TURTLE);

            String triplesString = convertModelToTriplesString(model, true);

            verboseDebug(System.out, triplesString);

            verboseDebug(System.out, "now as triples");

            ArrayList<Triple> triples = convertModelToTriplesArray(model, true);

            Iterator iter = triples.iterator();
            while (iter.hasNext()) {
              Triple triple = (Triple)iter.next();
              decodeTriple(triple);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * @g   named graph
     * @s   subject - all existing triples for this subject will be deleted
     * @triples - array of new triples to insert into the graph
     */
    public static void  replaceTriplesForSubject(MarkLogicWrapper wrapper, String g, String s, ArrayList<Triple> triples) {

        try {
            // this is now passed in
            // wrapper = getDatasetGraph();
            
            // see if this parses
            String deleteData = getPrefixesAsString()
                    + " "
                    + (g != null ? ("WITH " + g + " ") : "" )
                    + "DELETE {"
                    + " "
                    + "   ?s ?p ?o . "
                    + " "
                    + "} "
                    + "WHERE { ?s ?p ?o . "
                    + "  FILTER (?s = " + s + ") } " ;

            UpdateRequest update = UpdateFactory.create(deleteData);
            UpdateProcessor processor = UpdateExecutionFactory.create(update, wrapper.getDatasetGraph());
            processor.execute();

            // RDFDataMgr.write(System.out, wrapper.getGraph(NodeFactory.createURI("http://jameshillfarm.com/weather#yearly")), RDFFormat.TURTLE);


            String prefixStr = getPrefixesAsString();
            StringBuilder sb = new StringBuilder();
            sb.append(prefixStr);
            sb.append(" ");

            if (g != null)
                sb.append("WITH " + g + " ");
            sb.append( " ");
            sb.append( "INSERT {");
            sb.append( " " );
            sb.append( "\n" );

            Iterator iter = triples.iterator();
            while (iter.hasNext()) {
                Triple triple = (Triple)iter.next();
                sb.append(triple.getSubject().getURI());
                sb.append(" ");
                sb.append(triple.getPredicate().getURI());
                sb.append(" ");

                // RDFDatatype datatype = triple.getObject().asLiteral().getDatatype();
                Node obj = triple.getObject();
                if (obj.isLiteral()) {
                    // RDFNode literal = obj.asLiteral();
                    RDFDatatype datatype = obj.getLiteralDatatype();
                    Object o = obj.getLiteralValue();
                    // System.out.println("obj is " + obj.toString());
                    if (datatype != null)
                      System.out.println("datatype is " + datatype.toString());
                    // System.out.println("o is " + o.toString());
                    sb.append("\"");
                    sb.append(o.toString());
                    sb.append("\"");
                } else {
                    String uri = obj.getURI();
                    sb.append(uri);
                    sb.append(" ");
                }

                // just in case for now
                sb.append(" . \n");

            }
            // sb.append( "  wthr:today wthr:DummySeedKey \"2015-11-19T15:35:00\"^^xs:dateTime . ");

            sb.append( " ");
            sb.append( "} " );
            sb.append("\n");
            sb.append( " WHERE {} " );
            sb.append("\n");

            String insertData = sb.toString();

            // System.out.println("replaceTriplesForSubject:  inserting data:");
            // System.out.println(insertData);

            update = UpdateFactory.create(insertData);
            processor = UpdateExecutionFactory.create(update, wrapper.getDatasetGraph());
            processor.execute();

            // RDFDataMgr.write(System.out, wrapper.getGraph(NodeFactory.createURI("http://jameshillfarm.com/weather#yearly")), RDFFormat.TURTLE);


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void  insertTriplesForSubject(MarkLogicWrapper wrapper, String g, String s) {

        try {
            // this is now passed in
            // wrapper = getDatasetGraph();

            // RDFDataMgr.write(System.out, wrapper.getGraph(NodeFactory.createURI("http://jameshillfarm.com/weather#yearly")), RDFFormat.TURTLE);

            String insertData = "prefix wthr: <http://jameshillfarm.com/weather#> "
                    + "prefix xs: <http://www.w3.org/2001/XMLSchema#> "
                    + " "
                    + (g != null ? ("WITH " + g + " ") : "" )
                    + " "
                    + "INSERT DATA {"
                    + " "
                    + "  wthr:today wthr:DummySeedKey \"2015-11-19T15:34:00\"^^xs:dateTime . "
                    + " "
                    + "} " ;

            UpdateRequest update = UpdateFactory.create(insertData);
            UpdateProcessor processor = UpdateExecutionFactory.create(update, wrapper.getDatasetGraph());
            processor.execute();

            // RDFDataMgr.write(System.out, wrapper.getGraph(NodeFactory.createURI("http://jameshillfarm.com/weather#yearly")), RDFFormat.TURTLE);


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    protected static long countTriplesForGraph(MarkLogicWrapper wrapper, String g) {
        long count = 0;

        try {
            // this is now passed in
            // wrapper = getDatasetGraph();

            String pstr = getPrefixesAsString();

            String query = pstr 
                + " "
                + " select (count(?s) as ?total_triples) "
                + " where { "
                + "   graph " + g + " { "
                + "     ?s ?p ?o . "
                + "   } "
                + " } "
                + " group by ?s " ;

            QueryExecution execution = QueryExecutionFactory.create(
                    query, wrapper.getDatasetGraph().toDataset());

            int ii = 1;
            for (ResultSet results = execution.execSelect() ;
                    results.hasNext() ;
                    ii++ ) {
                QuerySolution solution = results.next();
                RDFDatatype datatype = solution.get("total_triples").asLiteral().getDatatype();
                String typeURI = "";
                if (datatype != null)
                    typeURI = datatype.getURI();

                String total_triples = solution.get("total_triples").asLiteral().getString();
                count = Long.parseLong(total_triples);

                // System.out.println("total triples:  " + count);
            }

            // wrapper.getDatasetGraph().close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return count;
    }


    protected static long displayTriplesForGraphSubject(MarkLogicWrapper wrapper, String g, String s) {
        long count = 0;

        try {
            // this is now passed in
            // wrapper = getDatasetGraph();

            String pstr = getPrefixesAsString();

            String query = pstr 
                + " "
                + " select ?s ?p ?o "
                + " where { "
                + (g != null ? ("   graph " + g + " { ") : "" )
                + "     ?s ?p ?o . "
                + "     filter( ?s = " + s + " ) "
                + (g != null ? ("   } ") : "" )
                + " } "
                + " group by ?s " ;

            QueryExecution execution = QueryExecutionFactory.create(
                    query, wrapper.getDatasetGraph().toDataset());

            int ii = 1;
            for (ResultSet results = execution.execSelect() ;
                    results.hasNext() ;
                    ii++ ) {
                QuerySolution solution = results.next();

                RDFDatatype datatype = solution.get("s").asLiteral().getDatatype();
                String typeURI = "";
                if (datatype != null)
                    typeURI = datatype.getURI();

                String total_triples = solution.get("total_triples").asLiteral().getString();
                count = Long.parseLong(total_triples);

                // System.out.println("total triples:  " + count);
            }

            // wrapper.getDatasetGraph().close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return count;
    }

    protected static long countTriplesForGraphSubject(MarkLogicWrapper wrapper, String g, String s) {
        long count = 0;

        try {
            // this is now passed in
            // wrapper = getDatasetGraph();

            String pstr = getPrefixesAsString();

            String query = pstr 
                + " \n"
                + " select (count(?s) as ?total_triples) "
                + " where { \n"
                + (g != null ? ("   graph " + g + " { \n") : "" )
                + "     ?s ?p ?o . \n"
                + "     filter( ?s = " + s + " ) \n"
                + (g != null ? ("   } \n") : "" )
                + " } \n"
                + " group by ?s \n" ;

            // System.out.println("countTriplesForGraphSubject:");
            // System.out.println(query);

            QueryExecution execution = QueryExecutionFactory.create(
                    query, wrapper.getDatasetGraph().toDataset());

            int ii = 1;
            for (ResultSet results = execution.execSelect() ;
                    results.hasNext() ;
                    ii++ ) {
                QuerySolution solution = results.next();
                RDFDatatype datatype = solution.get("total_triples").asLiteral().getDatatype();
                String typeURI = "";
                if (datatype != null)
                    typeURI = datatype.getURI();

                String total_triples = solution.get("total_triples").asLiteral().getString();
                count = Long.parseLong(total_triples);

                // System.out.println("total triples:  " + count);
            }

            // wrapper.getDatasetGraph().close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return count;
    }

    protected static void deleteTriplesForGraphSubject(MarkLogicWrapper wrapper, String g, String s) {

        try {
            // this is now passed in
            // wrapper = getDatasetGraph();

            String pstr = getPrefixesAsString();

            String query = pstr 
                + " "
                + (g != null ? (" WITH " + g + " ") : "" )
                + " DELETE { ?s ?p ?o . } "
                + " WHERE { "
                + "     ?s ?p ?o . "
                + "     FILTER( ?s = " + s + " ) "
                + " } " ;

            // System.out.println("deleteTriplesForGraphSubject:");
            // System.out.println(query);

            boolean doNow = true;
            if (doNow) {
                UpdateRequest update = UpdateFactory.create(query);
                UpdateProcessor processor = UpdateExecutionFactory.create(update, wrapper.getDatasetGraph());
                processor.execute();
            }

            // wrapper.getDatasetGraph().sync();
            // wrapper.getDatasetGraph().close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * just remembering how to do this
     */
    private static void getTriplesFromDocument() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // weatherCumulative.writeTurtleDocumentToStream(baos, "yearly", "subject", "year");

        // if this complains, one  can use baos.toString(Charset.defaultCharset())
        String str = baos.toString();

        Model m = ModelFactory.createDefaultModel();
        // make a reader from the string here
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

        m.read(bais, null, "TURTLE");

    }

/**
    TODO:  this is here because there is stuff that could be harvested from it

    public static boolean updateCurrentTurtle(MarkLogicWrapper wrapper, String filename) {
        if (filename == null)
            return false;

        if (debugMode)
            System.out.println("processing file " + filename);
        File f = new File(queueDirectory, filename);
        if (!f.exists()) {
            System.out.println("file does not exist:  " + filename);
            return false;
        }

        String graph = "wthr:current";
        // how we going to figure this one out?
        String subject = null;

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(f);
        } catch (FileNotFoundException e) {
            // not sure how this can happen - we checked above
            e.printStackTrace();
            return false;
        }

        Model m = null;
        try {
            m = ModelFactory.createDefaultModel();
            m.read(fis, null, "TURTLE");
        } catch (Exception e) {
            System.out.println("error parsing file " + filename);
            e.printStackTrace();
            return false;
        }

        // get the first statement, get the subject, figure out our subject
        // maybe call listSubjects() to see that they are all the same?
        ResIterator resIterator = m.listSubjects();
        Resource holder = null;
        Resource r = null;
        boolean singleSubject = true;
        while (resIterator.hasNext()) {
            r = resIterator.nextResource();
            if (holder == null)
                holder = r;
            if (!holder.equals(r)) {
                singleSubject = false;
                resIterator.close();
                break;
            }
        }

        if (!singleSubject) {
            System.out.println("this is not the only subject:  " + r.toString());
            return false;
        } else {
            // System.out.println("subject for current document:  " + holder.toString());
        }

        ArrayList<String> stmtArray = convertModelToStringArray(m, true);

        try {
            // this is now passed in
            // wrapper = getDatasetGraph();
            

            // RDFDataMgr.write(System.out, wrapper.getGraph(NodeFactory.createURI("http://jameshillfarm.com/weather#yearly")), RDFFormat.TURTLE);

            String insertData = getPrefixesAsString() 
                    + " "
                    + "insert data {"
                    + " \n" 
                    + "  graph " + graph + " {"
                    + " \n" ;
                    // + "  wthr:today wthr:DummySeedKey \"2015-11-19T15:34:00\"^^xs:dateTime . "

            // here we add all our statements
            StringBuilder sb = new StringBuilder();

            Iterator<String> iter = stmtArray.iterator();
            while (iter.hasNext()) {
                String stmt = iter.next();
                sb.append(" ");
                sb.append(stmt);
            }
            sb.append(" ");

            insertData = insertData
                    + sb.toString()
                    + "   }"
                    + " \n"
                    + "} \n" ;

            // System.out.println(insertData);

            boolean doNow = true;
            if (doNow) {
                UpdateRequest update = UpdateFactory.create(insertData);
                UpdateProcessor processor = UpdateExecutionFactory.create(update, wrapper.getDatasetGraph());
                processor.execute();
            }

            // RDFDataMgr.write(System.out, wrapper.getGraph(NodeFactory.createURI("http://jameshillfarm.com/weather#yearly")), RDFFormat.TURTLE);


        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

*/

/**
    TODO:  this is here just because there is stuff that can be harvested from it

    protected boolean replaceTriples(MarkLogicWrapper wrapper,
                                        String graph,
                                        String filename) {
        if (filename == null)
            return false;

        if (debugMode)
            System.out.println("processing file " + filename);
        File f = new File(queueDirectory, filename);
        if (!f.exists()) {
            System.out.println("file does not exist:  " + filename);
            return false;
        }

        String subject = null;

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(f);
        } catch (FileNotFoundException e) {
            // not sure how this can happen - we checked above
            e.printStackTrace();
            return false;
        }

        Model m = ModelFactory.createDefaultModel();
        m.read(fis, null, "TURTLE");

        // get the first statement, get the subject, figure out our subject
        // maybe call listSubjects() to see that they are all the same?
        ResIterator resIterator = m.listSubjects();
        Resource holder = null;
        Resource r = null;
        boolean singleSubject = true;
        while (resIterator.hasNext()) {
            r = resIterator.nextResource();
            if (holder == null)
                holder = r;
            if (!holder.equals(r)) {
                singleSubject = false;
                resIterator.close();
                break;
            }
        }

        if (!singleSubject) {
            System.out.println("this is not the only subject:  " + r.toString());
            return false;
        } else {
            if (debugMode)
                System.out.println("subject for current document:  " + r.toString());
        }

        ArrayList<String> stmtArray = convertModelToStringArray(m, true);

        // let's find out if there is data in there already
        subject = simplifyWithNamespace(r.toString());
        System.err.println("before simplification:  " + r.toString());
        System.err.println("after simplification:  " + subject);
        long existingTriples = countTriplesForGraphSubject(wrapper.getDatasetGraph(), graph, subject);
        if (debugMode)
            System.out.println("triples total for " + r.toString() + ":  " + existingTriples);

        try {
            

            // RDFDataMgr.write(System.out, wrapper.getGraph(NodeFactory.createURI("http://jameshillfarm.com/weather#yearly")), RDFFormat.TURTLE);

            String insertData = null;

            // easiest thing to do is delete the existing triples first

            if (existingTriples != 0) {
                deleteTriplesForGraphSubject(wrapper.getDatasetGraph(), graph, subject);
            }

            // this is now passed in
            // wrapper = getDatasetGraph();

            insertData = getPrefixesAsString() 
                    + " "
                    + "insert data {"
                    + " " 
                    + "  graph " + graph + " {"
                    + " " ;
                    // + "  wthr:today wthr:DummySeedKey \"2015-11-19T15:34:00\"^^xs:dateTime . "

            // here we add all our statements
            StringBuilder sb = new StringBuilder();

            Iterator<String> iter = stmtArray.iterator();
            while (iter.hasNext()) {
                String stmt = iter.next();
                sb.append(" ");
                sb.append(stmt);
            }
            sb.append(" ");

            insertData = insertData
                    + sb.toString()
                    + "   }"
                    + " "
                    + "} " ;

            // System.out.println(insertData);

            boolean doNow = true;
            if (doNow) {
                UpdateRequest update = UpdateFactory.create(insertData);
                UpdateProcessor processor = UpdateExecutionFactory.create(update, wrapper.getDatasetGraph());
                processor.execute();
            }

            // RDFDataMgr.write(System.out, wrapper.getGraph(NodeFactory.createURI("http://jameshillfarm.com/weather#yearly")), RDFFormat.TURTLE);


        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

*/


    public static boolean insertTriplesFromModel(MarkLogicWrapper wrapper,
                                        String graph,
                                        Model m) {

        String subject;

        // get the first statement, get the subject, figure out our subject
        // maybe call listSubjects() to see that they are all the same?
        ResIterator resIterator = m.listSubjects();
        Resource holder = null;
        Resource r = null;
        boolean singleSubject = true;
        while (resIterator.hasNext()) {
            r = resIterator.nextResource();
            if (holder == null)
                holder = r;
            if (!holder.equals(r)) {
                singleSubject = false;
                resIterator.close();
                break;
            }
        }

        if (!singleSubject) {
            System.out.println("this is not the only subject:  " + r.toString());
            return false;
        } else {
            if (debugMode)
                System.out.println("subject for current document:  " + r.toString());
        }

        ArrayList<String> stmtArray = convertModelToStringArray(m, true);

        // let's find out if there is data in there already
        subject = simplifyWithNamespace(r.toString());
        long existingTriples = countTriplesForGraphSubject(wrapper, graph, subject);
        if (debugMode)
            System.out.println("triples total for " + r.toString() + ":  " + existingTriples);


        try {
            // RDFDataMgr.write(System.out, wrapper.getGraph(NodeFactory.createURI("http://jameshillfarm.com/weather#yearly")), RDFFormat.TURTLE);

            String insertData = null;

            // easiest thing to do is delete the existing triples first

            if (existingTriples != 0) {
                deleteTriplesForGraphSubject(wrapper, graph, subject);
            }

            // this is now passed in
            // wrapper = getDatasetGraph();

            insertData = getPrefixesAsString() 
                    + " \n"
                    + "insert data {"
                    + " \n" 
                    + (graph != null ? ("  graph " + graph + " {") : "" )
                    + " \n" ;

            // here we add all our statements
            StringBuilder sb = new StringBuilder();

            Iterator<String> iter = stmtArray.iterator();
            while (iter.hasNext()) {
                String stmt = iter.next();
                sb.append(" ");
                sb.append(stmt);
            }
            sb.append(" ");

            insertData = insertData
                    + sb.toString()
                    + (graph != null ? ("   }") : "" )
                    + " \n"
                    + "} \n" ;

            // System.out.println(insertData);

            boolean doNow = true;
            if (doNow) {
                UpdateRequest update = UpdateFactory.create(insertData);
                UpdateProcessor processor = UpdateExecutionFactory.create(update,
                                                wrapper.getDatasetGraph());
                processor.execute();
            }

            // RDFDataMgr.write(System.out, wrapper.getGraph(NodeFactory.createURI("http://jameshillfarm.com/weather#yearly")), RDFFormat.TURTLE);


        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public static boolean insertTriplesFromInputStream(MarkLogicWrapper wrapper,
                                        String graph,
                                        InputStream instream) {

        Model m = ModelFactory.createDefaultModel();
        m.read(instream, null, "TURTLE");

        // get the first statement, get the subject, figure out our subject
        // maybe call listSubjects() to see that they are all the same?
        ResIterator resIterator = m.listSubjects();
        Resource holder = null;
        Resource r = null;
        boolean singleSubject = true;
        while (resIterator.hasNext()) {
            r = resIterator.nextResource();
            if (holder == null)
                holder = r;
            if (!holder.equals(r)) {
                singleSubject = false;
                resIterator.close();
                break;
            }
        }

        if (!singleSubject) {
            System.out.println("this is not the only subject:  " + r.toString());
            return false;
        } else {
            if (debugMode)
                System.out.println("subject for current document:  " + r.toString());
        }

        return insertTriplesFromModel(wrapper, graph, m);
    }

    public static boolean insertTriplesFromFile(MarkLogicWrapper wrapper,
                                        String graph,
                                        String filename) {
        if (filename == null)
            return false;

        if (debugMode)
            System.out.println("processing file " + filename);
        // File f = new File(queueDirectory, filename);
        File f = new File(filename);
        if (!f.exists()) {
            System.out.println("file does not exist:  " + filename);
            return false;
        }

        String subject = null;

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(f);
        } catch (FileNotFoundException e) {
            // not sure how this can happen - we checked above
            e.printStackTrace();
            return false;
        }

        return insertTriplesFromInputStream(wrapper, graph, fis);

    }

    public static ArrayList<Triple> constructTriplesForQuery(MarkLogicWrapper wrapper, String query) {
      ArrayList<Triple> triples = new ArrayList<Triple>();

        try {
            verboseDebug(System.out, query);

            QueryExecution execution = QueryExecutionFactory.create(
                    query, wrapper.getDatasetGraph().toDataset());

            // System.out.println("calling execConstruct");
            Model model = execution.execConstruct();
            // System.out.println("finished calling execConstruct");


            triples = convertModelToTriplesArray(model, true);

        } catch (Exception e) {
            e.printStackTrace();
        }

      return triples;
    }







    /**
     * TODO:  move this to ModelUtils
     * TODO:  make a version that converts it to an Array of Strings
     */
    protected static ArrayList<String> junk_convertModelToStringArray(Model m, boolean usePrefixes) {

        ArrayList<String> strings = new ArrayList<String>();

        StmtIterator stmtIterator = m.listStatements();
        while (stmtIterator.hasNext()) {
            Statement statement = stmtIterator.nextStatement();
            Resource subject = statement.getSubject();
            Property predicate = statement.getPredicate();
            RDFNode obj = statement.getObject();
            RDFDatatype datatype;
            String typeURI = "";

            StringBuilder sb = new StringBuilder();

            // System.out.println("subject:  " + subject.toString());
            // System.out.println("predicate:  " + predicate.toString());

            if (usePrefixes)
                sb.append(simplifyWithNamespace(subject.toString()));
            else
                sb.append(subject.toString());
            sb.append(" ");
            if (usePrefixes)
                sb.append(simplifyWithNamespace(predicate.toString()));
            else
                sb.append(predicate.toString());
            sb.append(" ");

            if (obj.isLiteral()) {
                Literal literal = obj.asLiteral();
                datatype = literal.getDatatype();
                if (datatype == null) {
                    String str = literal.getString();
                    // System.out.println("is plain literal:  str is " + str);
                    sb.append("\"");
                    sb.append(str);
                    sb.append("\"");
                    sb.append(" . \n");
                } else {
                    String dUri = datatype.getURI();
                    String str = literal.getString();
                    // System.out.println("is a typed literal:  dUri is " + dUri);
                    // System.out.println("    data is " + str);
                    sb.append("\"");
                    sb.append(str);
                    sb.append("\"^^");
                    if (usePrefixes)
                        sb.append(simplifyWithNamespace(dUri));
                    else
                        sb.append(dUri);
                    sb.append(" .\n");

                }
            } else {
                typeURI = obj.toString();
                // System.out.println("ERROR   --- ERROR");
                // System.out.println("not a literal:  typeURI = " + typeURI);
                sb.append(simplifyWithNamespace(typeURI));
                sb.append(" .\n");
            }

            strings.add(sb.toString());

        }

        return strings;

    }

    /**
     * this handles self-contained queries where the entire update is there
     * other modules handle doing a replace of all triples for a given subject
     * or graph/subject combination
     */
    public static void updateTripleQuery(MarkLogicWrapper wrapper, String query) {

      if ((wrapper == null) || (query == null))
        return;

      if (debugMode)
        System.out.println("updateTripleQuery:  " + query);

      boolean doNow = true;
      if (doNow) {
          UpdateRequest update = UpdateFactory.create(query);
          UpdateProcessor processor = UpdateExecutionFactory.create(update,
                                          wrapper.getDatasetGraph());
          processor.execute();
      }

    }

    private static String QUERY_TYPE_SELECT = "SELECT";
    private static String QUERY_TYPE_CONSTRUCT = "CONSTRUCT";
    private static String QUERY_TYPE_DESCRIBE = "DESCRIBE";
    private static String QUERY_TYPE_ASK = "ASK";

    public static String getQueryType(String query) {

      if (query == null)
        return null;

      StringTokenizer tokens = new StringTokenizer(query);
      while (tokens.hasMoreTokens()) {
        String token = tokens.nextToken();
        if (token.equalsIgnoreCase(QUERY_TYPE_SELECT))
          return QUERY_TYPE_SELECT;
        if (token.equalsIgnoreCase(QUERY_TYPE_CONSTRUCT))
          return QUERY_TYPE_CONSTRUCT;
        if (token.equalsIgnoreCase(QUERY_TYPE_DESCRIBE))
          return QUERY_TYPE_DESCRIBE;
        if (token.equalsIgnoreCase(QUERY_TYPE_ASK))
          return QUERY_TYPE_ASK;
      }

      return null;
    }

    public static String trimSurroundingQuotes(String str) {
      if (str == null)
        return str;

      String newstr = str;
      if (newstr.charAt(newstr.length()-1) == '\"')
        newstr = newstr.substring(0, newstr.length()-1);
      if (newstr.charAt(0) == '\"')
        newstr = newstr.substring(1);

      return newstr;
    }

    /**
     * these functions need to be deprecated
     */
    public static void addPrefix(String prefix, String iri) {
        PrefixMapper mapper = PrefixMapper.getPrefixMapper();
        mapper.addPrefix(prefix, iri);
    }

    public static void removePrefix(String prefix) {
        PrefixMapper mapper = PrefixMapper.getPrefixMapper();
        mapper.removePrefix(prefix);
    }

    public static Iterator getPrefixes() {

        PrefixMapper mapper = PrefixMapper.getPrefixMapper();
        return mapper.getPrefixes();
    }

    public static String getPrefixesAsString() {
        PrefixMapper mapper = PrefixMapper.getPrefixMapper();
        return mapper.getPrefixesAsString();
    }

    public static String getPrefixesAsTurtleString() {
        PrefixMapper mapper = PrefixMapper.getPrefixMapper();
        return mapper.getPrefixesAsTurtleString();
    }

    public static String expandPrefix(String s) {
        PrefixMapper mapper = PrefixMapper.getPrefixMapper();
        return mapper.expandPrefix(s);
    }

    public static String simplifyWithNamespace(String s) {
        PrefixMapper mapper = PrefixMapper.getPrefixMapper();
        return mapper.simplifyWithNamespace(s);
    }

}


