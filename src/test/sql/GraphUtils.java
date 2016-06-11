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



package test.sql;

import java.io.File;
import java.io.InputStream;
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

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.shared.PrefixMapping;

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

import test.utilities.MarkLogicWrapper;
import test.utilities.PrefixMapper;

/**
 * eventually this will be a static class
 */

public class GraphUtils {

    protected static final String PREFIX_PROPERTY_PREFIX = "prefixes.";
 
    private static final boolean debugFlag = false;

/*
    protected String propertiesFile = null;
    protected Properties properties = null;

    protected String host;
    protected int port;
    protected String user;
    protected String password;
    protected DatabaseClient client;
*/
    private static boolean debugMode = false;

    // this is temporary until prefixes move
    // protected String queueDirectory;

    public GraphUtils() {
    }

    public static void setDebugMode(boolean val) {
        debugMode = val;
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
     * TODO:  move this to ModelUtils
     * TODO:  make a version that converts it to an Array of Strings
     */
    protected static String convertModelToTriplesString(Model m, boolean usePrefixes) {

        StringBuilder sb = new StringBuilder();

        StmtIterator stmtIterator = m.listStatements();
        while (stmtIterator.hasNext()) {
            Statement statement = stmtIterator.nextStatement();
            Resource subject = statement.getSubject();
            Property predicate = statement.getPredicate();
            RDFNode obj = statement.getObject();
            RDFDatatype datatype;
            String typeURI = "";

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
                    sb.append(" . \r\n");
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
                    sb.append(" .\r\n");

                }
            } else {
                typeURI = obj.toString();
                // System.out.println("ERROR   --- ERROR");
                // System.out.println("not a literal:  typeURI = " + typeURI);
                sb.append(simplifyWithNamespace(typeURI));
                sb.append(" .\r\n");
            }
        }

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
                    sb.append(" . \r\n");
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
                    sb.append(" .\r\n");

                }
            } else {
                typeURI = obj.toString();
                // System.out.println("ERROR   --- ERROR");
                // System.out.println("not a literal:  typeURI = " + typeURI);
                sb.append(simplifyWithNamespace(typeURI));
                sb.append(" .\r\n");
            }

            strings.add(sb.toString());

        }

        return strings;

    }

    /**
     * @g   named graph
     * @s   subject - all existing triples for this subject will be deleted
     * @triples - array of new triples to insert into the graph
     */
    public static void  displayTriplesForSubject(MarkLogicWrapper wrapper, String g, String s) {


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

            System.out.println("calling execConstruct");
            Model model = execution.execConstruct();
            System.out.println("finished calling execConstruct");

            String triplesString = convertModelToTriplesString(model, true);

            // wrapper.getDatasetGraph().close();

            System.out.println(triplesString);

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
            String deleteData = "prefix wthr: <http://jameshillfarm.com/weather#> "
                    + "prefix xs: <http://www.w3.org/2001/XMLSchema#> "
                    + " "
                    + (g != null ? ("with " + g + " ") : "" )
                    + "delete {"
                    + " "
                    + "   ?s ?p ?o . "
                    + " "
                    + "} "
                    + "where { ?s ?p ?o . "
                    + "  filter (?s = " + s + ") } " ;

            UpdateRequest update = UpdateFactory.create(deleteData);
            UpdateProcessor processor = UpdateExecutionFactory.create(update, wrapper.getDatasetGraph());
            processor.execute();

            // RDFDataMgr.write(System.out, wrapper.getGraph(NodeFactory.createURI("http://jameshillfarm.com/weather#yearly")), RDFFormat.TURTLE);


            String prefixStr = getPrefixesAsString();
            StringBuilder sb = new StringBuilder();
            sb.append(prefixStr);
            sb.append(" ");

            if (g != null)
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

    }

    public static void  insertTriplesForSubject(MarkLogicWrapper wrapper, String g, String s) {

        try {
            // this is now passed in
            // wrapper = getDatasetGraph();

            // RDFDataMgr.write(System.out, wrapper.getGraph(NodeFactory.createURI("http://jameshillfarm.com/weather#yearly")), RDFFormat.TURTLE);

            String insertData = "prefix wthr: <http://jameshillfarm.com/weather#> "
                    + "prefix xs: <http://www.w3.org/2001/XMLSchema#> "
                    + " "
                    + (g != null ? ("with " + g + " ") : "" )
                    + " "
                    + "insert data {"
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
                + " \r\n"
                + " select (count(?s) as ?total_triples) "
                + " where { \r\n"
                + (g != null ? ("   graph " + g + " { \r\n") : "" )
                + "     ?s ?p ?o . \r\n"
                + "     filter( ?s = " + s + " ) \r\n"
                + (g != null ? ("   } \r\n") : "" )
                + " } \r\n"
                + " group by ?s \r\n" ;

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
                + (g != null ? (" with " + g + " ") : "" )
                + " delete { ?s ?p ?o . } "
                + " where { "
                + "     ?s ?p ?o . "
                + "     filter( ?s = " + s + " ) "
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
                    + " \r\n" 
                    + "  graph " + graph + " {"
                    + " \r\n" ;
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
                    + " \r\n"
                    + "} \r\n" ;

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
                    + " \r\n"
                    + "insert data {"
                    + " \r\n" 
                    + (graph != null ? ("  graph " + graph + " {") : "" )
                    + " \r\n" ;

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
                    + " \r\n"
                    + "} \r\n" ;

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


  /**
   * @g   named graph
   * @s   subject - all existing triples for this subject will be deleted
   * @triples - array of new triples to insert into the graph
   */
  public static Model  getModelForQuery(MarkLogicWrapper wrapper, String query) {

    Model model = null;

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
      System.out.println(query);

      QueryExecution execution = QueryExecutionFactory.create(
              query, wrapper.getDatasetGraph().toDataset());

      System.out.println("calling execConstruct");
      model = execution.execConstruct();
      System.out.println("finished calling execConstruct");

      if (debugFlag) {
        String triplesString = convertModelToTriplesString(model, true);
        System.out.println(triplesString);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return model;
  }

  public static Graph  getGraphForQuery(MarkLogicWrapper wrapper, String query) {
    Model m = getModelForQuery(wrapper, query);
    Graph g = m.getGraph();

    return g;
  }

    /**
     * TODO:  move this to ModelUtils
     * TODO:  make a version that converts it to an Array of Strings
     */
    protected static ArrayList<Triple> convertModelToTripleArray(Model m, boolean usePrefixes) {

        ArrayList<Triple> triples = new ArrayList<Triple>();

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
                    sb.append(" . \r\n");
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
                    sb.append(" .\r\n");

                }
            } else {
                typeURI = obj.toString();
                // System.out.println("ERROR   --- ERROR");
                // System.out.println("not a literal:  typeURI = " + typeURI);
                sb.append(simplifyWithNamespace(typeURI));
                sb.append(" .\r\n");
            }

/*
            strings.add(sb.toString());
*/
            Triple triple = statement.asTriple();
            triples.add(triple);
        }

        return triples;
    }

  public static ArrayList<Triple>  getTriplesForQuery(MarkLogicWrapper wrapper, String query) {
    Model m = getModelForQuery(wrapper, query);

    ArrayList<Triple> triples = convertModelToTripleArray(m, false);

    return triples;
  }

}


