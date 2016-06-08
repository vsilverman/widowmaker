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
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;


public class InferenceJenaData {
  protected Model resultsModel = null;
  protected Model responseModel = null;
  protected String triplesFilename = null;
  private HashMap<String, String> prefixes = null;
  
  public InferenceJenaData() {
    prefixes = new HashMap<String, String>();
  }

  public InferenceJenaData(HashMap cannedPrefixes) {
    prefixes = (HashMap)cannedPrefixes.clone();
    
  }

  public InferenceJenaData(HashMap cannedPrefixes, String xml) {
    prefixes = (HashMap)cannedPrefixes.clone();
    
  }

  public void setTriplesFile(String fname) {
    triplesFilename = fname;
  }

  public String getTriplesFile() {
    return triplesFilename;
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

  public void loadResults(String triples) {

    Model m = ModelFactory.createDefaultModel();
    Resource configuration = m.createResource();
    configuration.addProperty(ReasonerVocabulary.PROPruleMode, "hybrid");
    // configuration.addProperty(ReasonerVocabulary.PROPruleSet, getRulesFile());
    
    Reasoner reasoner = GenericRuleReasonerFactory.theInstance().create(configuration);

    Model data = FileManager.get().loadModel(getTriplesFile());
    resultsModel = ModelFactory.createInfModel(reasoner, data);

  }

  public void loadResponse(String triples) {
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

  public boolean compareModels() {
    return true;
  }

  public static void main(String[] args) {

    InferenceJenaData data = new InferenceJenaData();

    String prefixes = new String();
    String triples = new String();

    data.loadPrefixes("prefixes.ttl");
    data.setTriplesFile("triples.ttl");
    // data.loadResultsTriples("triples.ttl");
    data.loadResults("triples.rdf");

    data.writeResults();

  }
}

