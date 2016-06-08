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


package test.stress;

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
import java.util.Set;



public class ValidationManager
    extends Thread {
  protected ArrayList workLoad = null;
  protected HashMap<String, Model> resultsModels = null;
  // protected ResultsLogger logger = null;
  private boolean initialized = false;

  public ValidationManager() {
    workLoad = new ArrayList<Validator>();
    resultsModels = new HashMap<String, Model>();
    System.out.println("ValidationManager constructor");
  }

  public boolean isInitialized() {
    return initialized;
  }

  public Model getResultsModel(String queryUri) {
    Model m = null;

    m = resultsModels.get(queryUri);

    return m;
  }

  public String makeQueryUri(int queryId) {
    String s = "inf://query" + queryId;

    return s;
  }

  public void addResultsModel(int queryId, String results) {

    System.out.println("ValidationManager.addResultsModel for query " + queryId);

    if (results == null)
      return;
    InferenceQueryValidator v = new InferenceQueryValidator(this);
    v.loadResults(results);
    Model m = v.getResultsModel();
    String queryUri = makeQueryUri(queryId);
    resultsModels.put(queryUri, m);
  }

  public void loadResults(InferenceTestData data) {
    Set<Integer> keyset = data.results.keySet();
    Iterator<Integer>keys = keyset.iterator();
    while (keys.hasNext()) {
      Integer key = keys.next();
      InferenceResults infResult = (InferenceResults)data.results.get(key);
      int index = infResult.getIndex();
      String result = infResult.toString();
      addResultsModel(index, result);
    }

  }

  public void addWork(Validator validator) {
    synchronized (workLoad) {
      workLoad.add(validator);
    }
    this.interrupt();
  }

  public void validateThis(int queryId, String response) {

    try {
    InferenceQueryValidator v = new InferenceQueryValidator(this, queryId, response);
    v.validateThis(queryId, response);
    addWork(v);

    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("received exception on this response:");
      System.err.println(response);
    }
  }

  public void validateThis(int queryId,
                            String response,
                            String logFileName,
                            ResultsLogEntry logEntry ) {

    InferenceQueryValidator v = new InferenceQueryValidator(this, queryId, response);
    v.validateThis(queryId, response);
    v.setLogFileName(logFileName);
    v.setLogEntry(logEntry);
    addWork(v);
  }

  public void validateThis(Validator v) {
    if (v != null)
      addWork(v);
  }

  public void run() {

    initialized = true;

    while (true) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        // System.out.println("InferenceQueryValidator woke up");
      } catch (Exception e) {
        e.printStackTrace();
      }
      Validator v = null;
      synchronized(workLoad) {
        if (!workLoad.isEmpty()) {
          v = (Validator)workLoad.remove(0);
        }
      }
      if (v != null) {
        boolean result = v.doValidation();
        v.logResult(result);

      }
    }
  }
  
}

