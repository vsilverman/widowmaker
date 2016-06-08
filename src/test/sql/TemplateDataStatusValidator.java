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
import java.util.Date;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import test.stress.StressManager;
import test.stress.ResultsLogger;
import test.stress.ResultsLogEntry;
import test.stress.Validator;
import test.stress.ValidationManager;

import test.utilities.GraphUtils;
import test.utilities.PrefixMapper;

// eventually this extends a base class
public class TemplateDataStatusValidator
    implements Validator {
  protected Model masterModel = null;
  protected Model responseModel = null;
  protected String verifyResponse = null;
  protected String resultsQuery = null;
  protected String triplesFilename = null;
  protected ResultsLogEntry logEntry = null;
  protected String logFileName = null;
  protected ValidationManager manager = null;
  protected UpdateStatusRecord record = null;
  protected int queryId;
  
  public TemplateDataStatusValidator() {
  }

  public TemplateDataStatusValidator(ValidationManager manager) {
  }

  public TemplateDataStatusValidator(ValidationManager manager, String responseRdf) {
      this.manager = manager;
      loadResponse(responseRdf);
  }

  public TemplateDataStatusValidator(ValidationManager manager,
                                      String responseRdf,
                                      UpdateStatusRecord record) {
      this.manager = manager;
      // this.record = record;
      setUpdateStatusRecord(record);
      loadResponse(responseRdf);
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

  public Model getMasterModel() {
    return masterModel;
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

  public void setUpdateStatusRecord(UpdateStatusRecord record) {
    if (record == null)
      return;

    this.record = record;

    // make a string containing the triples for the record
    StringBuilder sb = new StringBuilder();
    sb.append(PrefixMapper.getPrefixMapper().getPrefixesAsTurtleString());
    sb.append("\n");
    sb.append("link:ValidationCheck ");
    sb.append("link:LastTransactionStatus ");
    sb.append("\"" + record.newStatus + "\" .");
    sb.append("\n");
    sb.append("link:ValidationCheck ");
    sb.append("link:TransactionUri ");
    sb.append("\"" + record.primaryUri + "\" .");
    sb.append("\n");
/*
    if (record.lastDate != null) {
      sb.append("link:ValidationCheck ");
      sb.append("link:LastTransactionDate ");
      sb.append("\"" + record.lastDate + "\"^^xs:dateTime .");
      sb.append("\n");
    }
*/

    loadMaster(sb.toString());
  }

  public boolean compareResults(UpdateStatusRecord record) {
    boolean bval = true;

    if (record == null)
      return false;
    if (responseModel == null)
      return false;
    
    // check the status

    // check the date

    
    return bval;
  }

  public boolean compareResults() {
    return compareResults(record);
  }

  public void loadMasterTriples(String filename) {
    Model model = ModelFactory.createDefaultModel();
    try {
    model.read(new FileInputStream(filename), null, "TTL");

    masterModel = model;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  public void loadMaster(String rdf) {

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

    masterModel = m;
  }

  public void loadResponse(String rdf) {

    // make an model from the results;
    Model m = ModelFactory.createDefaultModel();
    Resource configuration = m.createResource();
    
    // stuff in my prefixes before getting to work
    // this returns an iterator of prefix keys, not the prefixes
    Iterator iter = PrefixMapper.getPrefixMapper().getPrefixes();
    while (iter.hasNext()) {
      String prefix = (String)iter.next();
      String ns = (String)PrefixMapper.getPrefixMapper().getNamespace(prefix);
      m.setNsPrefix(prefix, ns);
    }

    Reasoner reasoner = GenericRuleReasonerFactory.theInstance().create(configuration);

    InputStream is = new ByteArrayInputStream(rdf.getBytes());

    try {
      m.read(is, null, "TURTLE");
    } catch (RiotException e ) {
      // probably an empty response
      // whatever it is, it won't parse!
      System.err.println("Error parsing validation response. Continuing with empty model.");
      e.printStackTrace();
    }
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
    StmtIterator iter = masterModel.listStatements();
  
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
    
    RDFDataMgr.write(System.out, masterModel, RDFFormat.TURTLE);
  }

  public void writeResultsRDF() {
    
    RDFDataMgr.write(System.out, masterModel, RDFFormat.RDFXML);
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

  public boolean compareModels(ArrayList masterList, ArrayList responseList) {

    if ((masterList == null) || (responseList == null))
      return false;

    // triples missing from the results that were present in the response
    ArrayList extraTriples = new ArrayList<Statement>();
    ArrayList masterClone = (ArrayList)masterList.clone();
    // I don't think we need to clone this one - it's not a master
    ArrayList responseClone = (ArrayList)responseList.clone();

    Iterator i = responseClone.iterator();
    while (i.hasNext()) {
      Statement stmt = (Statement)i.next();
      int match = masterClone.indexOf(stmt);
      if (match > -1) {
        masterClone.remove(match);
      } else {
        extraTriples.add(stmt);
      }
    }
    boolean rval = true;
    // responseClone should now be empty
    int size = masterClone.size();
    if (size > 0) {
      System.out.println(new Date().toString());
      System.out.println("record:  " + record.primaryUri);
      System.out.println("masterClone still has triples:  " + size);
      rval = false;
      dumpList(masterClone);
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
   * does the actual validation work
   */
  public boolean doValidation() {

    // System.out.println("Validator.doValidation");

    boolean bval;
    // temporary flag to get test to run while validation didn't work
    boolean doWork = true;
    if (doWork) {
      if (masterModel == null)
        System.out.println("masterModel is null");
      if (responseModel == null)
        System.out.println("responseModel is null");

      ArrayList<Statement> masterList = makeList(masterModel);
      ArrayList<Statement> responseList = makeList(responseModel);

      bval = compareModels(masterList, responseList);
    } else {
      bval = true;
    }

    return bval;

  }

  public void logResult(boolean result) {
    ResultsLogger logger = (ResultsLogger)StressManager.getResultsLogger(logFileName);
    if (logger != null) {
      // TODO:  temporary just to log what happens for now to see when LOAD is taking place
      // String newId = "VerifyStatus " + logEntry.getIdentifier();
      // logEntry.setIdentifier(newId);
      logEntry.setPassFail(result);
      logger.logResult(logEntry);
    }
  }

  public static void main(String[] args) {

    TemplateDataStatusValidator data = new TemplateDataStatusValidator();

    String prefixes = new String();
    String triples = new String();

    // data.loadPrefixes("prefixes.ttl");
    data.setTriplesFile("triples.ttl");
    data.loadMasterTriples("triples.ttl");
    // data.loadMaster("triples.rdf");

    data.writeResults();
    data.writeResultsRDF();
    data.walkResults();

  }
}

