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


package test.stress;

import java.lang.IllegalArgumentException;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.*;
import com.hp.hpl.jena.shared.*;
import com.hp.hpl.jena.util.*;
import com.hp.hpl.jena.graph.*;
import com.hp.hpl.jena.graph.impl.*;
import com.hp.hpl.jena.ontology.*;
import com.hp.hpl.jena.reasoner.*;
import com.hp.hpl.jena.reasoner.rulesys.*;
import org.apache.jena.riot.*;

import java.io.PrintWriter;
import java.io.PrintStream;
import java.io.OutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;


// eventually this extends a base class
public class InferenceLoadValidator
    implements Validator {
  protected Model resultsModel = null;
  protected Model responseModel = null;
  protected String verifyResponse = null;
  protected String resultsQuery = null;
  protected String triplesFilename = null;
  private HashMap<String, String> prefixes = null;
  protected ResultsLogEntry logEntry = null;
  protected String logFileName = null;
  protected int queryId;
  
  public InferenceLoadValidator() {
    prefixes = new HashMap<String, String>();
  }

  public InferenceLoadValidator(HashMap cannedPrefixes) {
    prefixes = (HashMap)cannedPrefixes.clone();
    
  }

  public InferenceLoadValidator(HashMap cannedPrefixes, String xml) {
    prefixes = (HashMap)cannedPrefixes.clone();
    
  }

  public InferenceLoadValidator(ValidationManager manager) {
    prefixes = new HashMap<String, String>();
  }

  public InferenceLoadValidator(ValidationManager manager,
              int queryId,
              String response) {
    prefixes = new HashMap<String, String>();
    this.queryId = queryId;
    loadResponse(response);
  }

  public InferenceLoadValidator( int queryId,
              String response) {
    prefixes = new HashMap<String, String>();
    this.queryId = queryId;
    loadResponse(response);
  }

  public void setLogEntry(ResultsLogEntry entry) {
    logEntry = entry;
  }

  public ResultsLogEntry getLogEntry() {
    return logEntry;
  }

  public void setLogFileName(String fname) {
    logFileName = fname;
  }

  public String getLogFileName() {
    return logFileName;
  }

  public void setTriplesFile(String fname) {
    triplesFilename = fname;
  }

  public String getTriplesFile() {
    return triplesFilename;
  }

  public Model getResultsModel() {
    return resultsModel;
  }

  public Model getResponseModel() {
    return responseModel;
  }

  public void setVerifyResponse(String response) {
    verifyResponse = response;
  }

  public void setResultsQuery(String query) {
    resultsQuery = query;
  }

  // TODO:  how do we load these?
  public void loadPrefixes(String prefixFilename) {
    BufferedReader br = null;
    StringTokenizer tokens = null;

    try {
      br = new BufferedReader(new FileReader(new File(prefixFilename)));
      String line = null;
      while ((line = br.readLine()) != null) {
        // parse the line, look for @prefix on front
        tokens = new StringTokenizer(line);
        if (tokens.hasMoreElements()) {
          // check the first token explicitly, decide if it's a prefix
          String str = (String)tokens.nextElement();
          if (str.equalsIgnoreCase("@prefix")) {
            // prefix followed by URI
            // TODO: is this right? doesn't look right
            String prefix = (String)tokens.nextElement();
            String uri = (String)tokens.nextElement();
            System.out.println("str, prefix, uri = " + str + ", " + prefix + ", " + uri);
            if ((prefix.length() > 0) && (uri.length() > 0)) {
              prefixes.put(prefix, uri);
            }
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }


  }

  public void loadResultsTriples(String filename) {
    Model model = ModelFactory.createDefaultModel();
    try {
    model.read(new FileInputStream(filename), null, "TTL");

    resultsModel = model;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  public void loadResults(String rdf) {

    Model m = ModelFactory.createDefaultModel();
    Resource configuration = m.createResource();
    
    Reasoner reasoner = GenericRuleReasonerFactory.theInstance().create(configuration);

    InputStream is = new ByteArrayInputStream(rdf.getBytes());
    try {
    m.read(is, null, "TURTLE");
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("exception  caught parsing results:");
      System.err.println(rdf);
    }

    resultsModel = m;
  }

  public void loadResponse(String rdf) {

    // make an model from the results;
    Model m = ModelFactory.createDefaultModel();
    Resource configuration = m.createResource();
    
    Reasoner reasoner = GenericRuleReasonerFactory.theInstance().create(configuration);

    InputStream is = new ByteArrayInputStream(rdf.getBytes());

    m.read(is, null, "TURTLE");
    responseModel = m;
  }

  public ArrayList makeList(Model m) {

    if (m == null)
      return null;

    ArrayList<Statement>list = new ArrayList<Statement>();

    StmtIterator iter = m.listStatements();
  
    while (iter.hasNext()) {
      Statement stmt = iter.nextStatement();
      Resource subject = stmt.getSubject();
      Property predicate = stmt.getPredicate();
      RDFNode object = stmt.getObject();

      list.add(stmt);

    }

    return list;
  }

  public void walkResults() {
    StmtIterator iter = resultsModel.listStatements();
  
    while (iter.hasNext()) {
      Statement stmt = iter.nextStatement();
      Resource subject = stmt.getSubject();
      Property predicate = stmt.getPredicate();
      RDFNode object = stmt.getObject();

      System.out.print("?s ?p ?o:  " + subject.toString() + " ");
      System.out.print(predicate.toString() + " ");
      if (object instanceof Resource)
        System.out.print(object.toString());
      else {
        // object is literal
        System.out.print("\"" + object.toString() + "\"");
      }
      System.out.println(" .");
    }
  }

  public void writeResults() {
    
    RDFDataMgr.write(System.out, resultsModel, RDFFormat.TURTLE);
  }

  public void writeResultsRDF() {
    
    RDFDataMgr.write(System.out, resultsModel, RDFFormat.RDFXML);
  }

  private void dumpList(ArrayList list) {
    Iterator iter = list.iterator();
  
    while (iter.hasNext()) {
      Statement stmt = (Statement)iter.next();
      Resource subject = stmt.getSubject();
      Property predicate = stmt.getPredicate();
      RDFNode object = stmt.getObject();

      System.out.print("?s ?p ?o:  " + subject.toString() + " ");
      System.out.print(predicate.toString() + " ");
      if (object instanceof Resource) {
        System.out.print(object.toString());
      } else {
        // object is literal
        System.out.print("\"" + object.toString() + "\"");
      }
      System.out.println(" .");
    }
  }

  public boolean compareModels(ArrayList resultsList, ArrayList responseList) {

    if ((resultsList == null) || (responseList == null))
      return false;

    // triples missing from the results that were present in the response
    ArrayList extraTriples = new ArrayList<Statement>();
    ArrayList resultsClone = (ArrayList)resultsList.clone();
    // I don't think we need to clone this one - it's not a master
    ArrayList responseClone = (ArrayList)responseList.clone();

    Iterator i = responseClone.iterator();
    while (i.hasNext()) {
      Statement stmt = (Statement)i.next();
      int match = resultsClone.indexOf(stmt);
      if (match > -1) {
        resultsClone.remove(match);
      } else {
        extraTriples.add(stmt);
      }
    }
    boolean rval = true;
    // responseClone should now be empty
    int size = resultsClone.size();
    if (size > 0) {
      System.out.println("resultsClone still has triples:  " + size);
      rval = false;
      dumpList(resultsClone);
    }
    size = extraTriples.size();
    if (size > 0) {
      System.out.println("extraTriples has triples:  " + size);
      rval = false;
      dumpList(extraTriples);
    }

    return rval;
  }

  /**
   * pass in the query ID (query number) and the results string that was the
   * response
   */
  public void validateThis(int queryID, String response) {
    String queryUri;

    queryUri = "inf://query" + Integer.toString(queryID);

    loadResponse(response);
    // let's do some work
    resultsModel = StressManager.validationMgr.getResultsModel(queryUri);
    if (resultsModel == null) {
      System.out.println("fatal error:  unable to find master results for query " + queryUri);
      return;
    }

    if (responseModel == null) {
      System.out.println("fatal error:  unable to find response model for query " + queryUri);
    }

  }

  /**
   * does the actual validation work
   */
  public boolean doValidation() {

    boolean bval;
    // temporary flag to get test to run while validation didn't work
    boolean doWork = true;
    if (doWork) {
      loadResponse(verifyResponse);
      loadResults(resultsQuery);

    ArrayList<Statement> resultsList = makeList(resultsModel);
    ArrayList<Statement> responseList = makeList(responseModel);

    bval = compareModels(resultsList, responseList);
    } else {
      bval = true;
    }

    return bval;

  }

  public void logResult(boolean result) {
    ResultsLogger logger = (ResultsLogger)StressManager.getResultsLogger(logFileName);
    if (logger != null) {
      // TODO:  temporary just to log what happens for now to see when LOAD is taking place
      String newId = "LOAD " + logEntry.getIdentifier();
      logEntry.setIdentifier(newId);
      logEntry.setPassFail(result);
      logger.logResult(logEntry);
    }
  }

  public static void main(String[] args) {

    InferenceLoadValidator data = new InferenceLoadValidator();

    String prefixes = new String();
    String triples = new String();

    data.loadPrefixes("prefixes.ttl");
    data.setTriplesFile("triples.ttl");
    data.loadResultsTriples("triples.ttl");
    // data.loadResults("triples.rdf");

    data.writeResults();
    data.writeResultsRDF();
    data.walkResults();

  }
}

