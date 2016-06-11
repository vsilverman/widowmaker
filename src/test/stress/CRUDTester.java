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

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.exceptions.RetryableXQueryException;

public class CRUDTester extends XccLoadTester {
  // include data class here
  protected CRUDTestData crudTestData = null;
  protected int numDeleted = 0;
  protected int numUpdated = 0;
  protected int loopCnt = 0;
  protected String[] miscStr = { "Aggregate", "Enable", "Leverage",
      "Facilitate", "Synergize", "Repurpose", "Strategize", "Reinvent",
      "Harness", "cross-platform", "best-of-breed", "frictionless",
      "ubiquitous", "extensible", "compelling", "mission-critical",
      "collaborative", "integrated", "methodologies", "infomediaries",
      "platforms", "schemas", "mindshare", "paradigms", "functionalities",
      "web services", "infrastructures" };


  class CrudValidationNotifier
    extends ReplicaValidationNotifierImpl
  {
    private ValidationData vData = null;
    String  operation = null;
    String  URI = null;

    CrudValidationNotifier()
    {
      super();
    }

    CrudValidationNotifier(ValidationData data)
    {
      super();
      vData = data;
    }

    void  setValidationData(ValidationData data) {
      vData = data;
    }

    void  setCrudOperation(String op) {
      operation = op;
    }

    String  getCrudOperation() {
      return operation;
    }
  
    void  setURI(String uri) {
      URI = uri;
    }

    String  getURI() {
      return URI;
    }

    public synchronized void notifyReplicaValidationComplete() {
      long totalTime = System.currentTimeMillis() - createMillis;
      long runTime = System.currentTimeMillis() - startMillis;
      System.out.println("CrudReplicaValidationNotifier.notifyComplete " + URI + " " + operation + " " + result + ", " + runTime + ", " + totalTime);
      finished = true;
      notifyAll();
    }

    public synchronized void notifyReplicaValidationComplete(boolean result) {
      setReplicaValidationPassed(result);
      this.notifyReplicaValidationComplete();
    }

  }

  public CRUDTester(ConnectionData connData, CRUDTestData crudData,
      String threadName) {
    super(connData, (LoadTestData) crudData, threadName);
    crudTestData = crudData;

  }

  private String getContent() {
    int numStrings = (int) (Math.random() * miscStr.length);
    String retStr = "";
    for (int i = 0; i < numStrings; i++) {
      int stringPos = (int) (Math.random() * miscStr.length);
      if (i == 0) {
        retStr += miscStr[stringPos];
      } else {
        retStr += " " + miscStr[stringPos];
      }
    }
    return retStr;

  }
  private void updateURIJS(String URI){
    String jsStr;
    String updateStr = "declareUpdate(); \n"; 
    if (crudTestData.isJSONTest()) {
  
      jsStr =
  
  "var d = cts.doc('" +  URI + "') \n" + 
  "var obj = d.toObject()  \n" +  
      "if (obj['update-ele']) { \n" + 
  "      obj['update-ele'].push('" + getContent() + "'); \n" + 
  "    }else { \n" + 
  "      obj['update-ele'] = ['" + getContent() + "'] \n" +  
      "} \n" + 
  "xdmp.documentInsert('" + URI + "', obj) "; 
     }else { 
  jsStr = 
    "var doc =  cts.doc('" + URI + "'); \n" + 
    "var xml = xdmp.unquote('" + getUpdateElement() + "').next().value.root; \n" +  
    "xdmp.nodeInsertChild( doc.xpath('/*') , xml) "; 
     }
    if( ! testData.getMultiStatement() ) jsStr = updateStr + jsStr; 
  
    for (SessionHolder s : sessions) {
      long startTime = System.currentTimeMillis();
  try{
      s.runQuery(new Query(jsStr, "javascript"), null);
  }catch(Exception e) {
      e.printStackTrace();
  }
      totalTime += System.currentTimeMillis() - startTime;
    }
  }
  private void updateURI(String URI) {
    String xqStr;
    if (crudTestData.isJSONTest()) {
      xqStr = 
        " let $d := fn:doc('" + URI + "') return " + 
        " if (fn:count($d/array-node('update-ele')) eq 0) " +
        " then xdmp:node-insert-child($d/object-node() , object-node { 'update-ele' : array-node { text {'" +
        getContent() + "'} } }/array-node('update-ele'))" + 
        " else xdmp:node-insert-child($d/array-node('update-ele'), text {'" + getContent() + "'})" ;
    } else {
      xqStr = "xdmp:node-insert-child( fn:doc('" + URI + "')/* , "
        + getUpdateElement() + ")";
    }
    for (SessionHolder s : sessions) {
      long startTime = System.currentTimeMillis();
      s.runQuery(xqStr);
      totalTime += System.currentTimeMillis() - startTime;
    }
  }

  private String getUpdateValidationQuery(String URI) {
    return "fn:count(fn:doc('" + URI + "')//update-ele )";
  }

  private String getUpdateElement() {
    return "<update-ele>" + getContent() + "</update-ele >";
  }

  protected void verifyUpdated(String verificationQuery, int expected,
      String curURI) throws Exception {
    int serverIndex = 0;
    for (SessionHolder s : sessions) {
      String value = getValueFromResults(s.runQuery(verificationQuery, null));
      int numInDB = Integer.parseInt(value);
      if (expected != numInDB) {
        String error = "ERROR number of elements after update FAIL expected"
            + expected + " found " + numInDB + " URI " + curURI;
        s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      } else { // query validated fine against server so add to replica
               // validation pool
        if (connData.servers.get(serverIndex).replicas.size() > 0) {
          // if we handle lag=0 testing, we don't send the timestamp
          String timeStamp = getValueFromResults(s.runQuery(
              "xdmp:request-timestamp()", null));
          CrudValidationNotifier notifier = new CrudValidationNotifier();
          ValidationData vData = new ValidationData(
                  connData, verificationQuery, value, timeStamp, serverIndex, notifier);
          notifier.setValidationData(vData);
          notifier.setURI(uniqueURI);
          notifier.setCrudOperation("Update");
          StressManager.replicaValidationPool.addValidation(vData);
          // notifier.waitForComplete();
        }
      }
      serverIndex++;
    }
  }

  protected void verifyUpdateInterval() throws Exception {
    int serverIndex = 0;
  System.out.println("verifyUpdateInterval");
    for (SessionHolder s : sessions) {
      String xqStr = "fn:count(fn:collection('" + uniqueURI + "')//update-ele)";
      String value = getValueFromResults(s.runQuery(xqStr, null));
      int numInDB = Integer.parseInt(value);
      if (numInDB != numUpdated) {
        String error = "ERROR update test failed expected " + numUpdated
            + " from  collection " + uniqueURI + " \n db contained " + numInDB;
        s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      } else { // query validated fine against server so add to replica
               // validation pool
        if (connData.servers.get(serverIndex).replicas.size() > 0) {
          String timeStamp = getValueFromResults(s.runQuery(
                           "xdmp:request-timestamp()", null));
          CrudValidationNotifier notifier = new CrudValidationNotifier();
          ValidationData vData = new ValidationData(
                  connData, xqStr, value, timeStamp, serverIndex, notifier);
          notifier.setValidationData(vData);
          notifier.setURI(uniqueURI);
          notifier.setCrudOperation("Update");
          StressManager.replicaValidationPool.addValidation(vData);
          notifier.waitForComplete();
        }
      }
      serverIndex++;
    }
  }
  protected void verifyUpdateIntervalJS() throws Exception {
    int serverIndex = 0;
  System.out.println("verifyUpdateIntervalJS");
    for (SessionHolder s : sessions) {
      String jsStr = "var count = 0; \n" +  
      "for ( var x of fn.collection('"+ uniqueURI +"')) { \n" + 
        "  count += fn.count( x.xpath('//update-ele')); \n" + 
      "} \n" + 
         "count";
      String xqStr = "fn:count(fn:collection('" + uniqueURI + "')//update-ele)"; 
      String value = getValueFromResults(s.runQuery(new Query(jsStr, "javascript") , null));
      int numInDB = Integer.parseInt(value);
      if (numInDB != numUpdated) {
        String error = "ERROR update test failed expected " + numUpdated
            + " from  collection " + uniqueURI + " \n db contained " + numInDB;
        s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      } else {
        if (connData.servers.get(serverIndex).replicas.size() > 0) {
          String timeStamp = getValueFromResults(s.runQuery(
                            "xdmp:request-timestamp()", null));
          CrudValidationNotifier notifier = new CrudValidationNotifier();
          ValidationData vData = new ValidationData(
                  connData, xqStr, value, timeStamp, serverIndex, notifier);
          notifier.setValidationData(vData);
          notifier.setURI(uniqueURI);
          notifier.setCrudOperation("Update");
          StressManager.replicaValidationPool.addValidation(vData);
          notifier.waitForComplete();
        }
      }
      serverIndex++;
    }
  }
   protected void verifyDeleted(Query verificationQuery, int expected,
      String curURI) throws Exception {
    int serverIndex = 0;
    for (SessionHolder s : sessions) {
      String value = getValueFromResults(s.runQuery(verificationQuery, null));
      int numInDB = Integer.parseInt(value);
      if (expected != numInDB) {
        String error = "ERROR found URI after delete : " + curURI
            + " num in db " + numInDB + " num expected " + expected;
        s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      } else {// query validated fine against server so add to replica
        // validation pool
        if (connData.servers.get(serverIndex).replicas.size() > 0) {
          String timeStamp = getValueFromResults(s.runQuery(
                            "xdmp:request-timestamp()", null));
          CrudValidationNotifier notifier = new CrudValidationNotifier();
          ValidationData vData = new ValidationData(
                  connData, verificationQuery.query, value, timeStamp, serverIndex, notifier);
          notifier.setValidationData(vData);
          notifier.setURI(curURI);
          notifier.setCrudOperation("Delete");
          StressManager.replicaValidationPool.addValidation(vData);
        }
    else
      System.out.println("verifyDeleted(Query):  no replicas");
      }
      serverIndex++;
    }
  }

  protected void verifyDeleted(String verificationQuery, int expected,
      String curURI) throws Exception {
    int serverIndex = 0;
  // System.out.println("verifyDeleted(String)");
    for (SessionHolder s : sessions) {
      String value = getValueFromResults(s.runQuery(verificationQuery, null));
      int numInDB = Integer.parseInt(value);
      if (expected != numInDB) {
        String error = "ERROR found URI after delete : " + curURI
            + " num in db " + numInDB + " num expected " + expected;
        s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      } else { // query validated fine against server so add to replica
               // validation pool
        if (connData.servers.get(serverIndex).replicas.size() > 0) {
          String timeStamp = getValueFromResults(s.runQuery(
                          "xdmp:request-timestamp()", null));
          ValidationData vData = new ValidationData(
                  connData, verificationQuery, value, timeStamp, serverIndex);
          StressManager.replicaValidationPool.addValidation(vData);
        }
      }
      serverIndex++;
    }
  }

  protected void verifyDeleteInterval() throws Exception {
    int serverIndex = 0;
  // System.out.println("verifyDeleteInterval");
    for (SessionHolder s : sessions) {
      String xqStr = "fn:count(fn:collection('" + uniqueURI + "'))";
      String value = getValueFromResults(s.runQuery(xqStr, null));
      int numInDB = Integer.parseInt(value);
      if (numInDB != numLoaded - numDeleted) {
        String error = "ERROR delete test failed expected"
            + (numLoaded - numDeleted) + " from  collection " + uniqueURI
            + " contained " + numInDB;
        s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      } else { // query validated fine against server so add to replica
               // validation pool
        if (connData.servers.get(serverIndex).replicas.size() > 0) {
          // if we handle lag=0 testing, we don't send the timestamp
          String timeStamp = getValueFromResults(s.runQuery(
              "xdmp:request-timestamp()", null));
          CrudValidationNotifier notifier = new CrudValidationNotifier();
          ValidationData vData = new ValidationData(
                  connData, xqStr, value, timeStamp, serverIndex, notifier);
          notifier.setValidationData(vData);
          notifier.setURI(uniqueURI);
          notifier.setCrudOperation("Delete");
          StressManager.replicaValidationPool.addValidation(vData);
          notifier.waitForComplete();
        }
      }
      if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG"))
        s.runQuery(xqStr);

      serverIndex++;
    }
  }

  String[] getURIS(int loopUpdateCount) throws Exception {
    int batchSize = 10;
    if (crudTestData.getMultiStatement())
      batchSize = crudTestData.getBatchSize();
    String xqStr = "xquery version '1.0-ml';" + " let $docs := fn:collection('"
        + uniqueURI + "')[" + ((loopUpdateCount * batchSize) + 1) + " to "
        + ((loopUpdateCount * batchSize) + batchSize) + "]"
        + " for $doc in $docs" + " return " + " fn:base-uri($doc)";
    ResultSequence rs = sessions.get(0).runQuery(xqStr, null);
    String[] uris = new String[rs.size()];
    int pos = 0;
    try {
      while (rs.hasNext()) {
        uris[pos++] = rs.next().getItem().asString();
      }
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    } finally {
      try {
        rs.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return uris;
  }

  public void updateTest() throws InterruptedException {
    int retryCount = 0;
    String curURI;
    int i = 0;
    while (i < numLoaded && alive) {
      try {
        String[] uris = getURIS(i);
        beginTransaction();
        // if(uris.length == 0) i = numLoaded;
        int pos = 0;
        for (; pos < uris.length && alive; pos++) {
          curURI = uris[pos];
          if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG")) {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Date date = new Date();
            System.out.println("Updating " + curURI + " at " + dateFormat.format(date));
          }
          updateURI(uris[pos]);
        }
        if (!crudTestData.getRollback()) {
          commitTransaction(); beginTransaction();
          numUpdated += uris.length;
          for (int p = pos - 1; p >= 0; --p)
            verifyUpdated(getUpdateValidationQuery(uris[p]), loopCnt + 1,
                uris[p]);
        } else {
          rollbackTransaction();
          for (int p = pos - 1; p >= 0; --p)
            verifyUpdated(getUpdateValidationQuery(uris[p]), 0, uris[p]);
        }
        ++i;
        retryCount = 0;

        Thread.sleep(crudTestData.getMaxSleepTime());
        if (numUpdated % crudTestData.getCheckInterval() == 0
            && !crudTestData.getRollback()){
    if( isJSLoad ){
    verifyUpdateIntervalJS();
    }else{
                verifyUpdateInterval();
    }
  }
      } catch (RetryableXQueryException e) {
        try {
          rollbackTransaction();
        } catch (Exception Ei) {
        }
        if (++retryCount > 25) {
          System.out.println("Retries exhausted");
          e.printStackTrace();
          ++i;
          retryCount = 0;
        } else {
          System.out.println("Retrying: " + retryCount);
          Thread.sleep(1000);
        }
      } catch (Exception e) {
        e.printStackTrace();
        try {
    System.out.println(testData.toString()); 
          rollbackTransaction();
        } catch (Exception Ei) {
        }
        ++i;
        retryCount = 0;
      }
    }
  }

  public void deleteTest() throws InterruptedException {
    int retryCount = 0;
    String curURI;
    String verificationQuery = "";
    int i = 0;
    while (numDeleted < numLoaded && alive) {
      try {
        // delete uri here
        // because this is a delete test it should not increment the loop count because everything is 
        // always gone from each loop unless there is a bug
        String[] uris = getURIS(0); //Changfei: 01/18/2016 solve illegal transaction error in multistatement
        if (uris.length > 0) {  //Changfei: 01/18/2016 solve illegal transaction error in multistatement
        beginTransaction();
        int pos = 0;
        for (; pos < uris.length && alive; pos++) {
          curURI = uris[pos];
        if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG")) {
          DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          Date date = new Date();
          System.out.println("Deleting " + curURI + " at " + dateFormat.format(date));
        }
          String query = "xdmp:document-delete('" + curURI + "')";
          for (SessionHolder s : sessions){
      if(isJSLoad){
    if( ! testData.getMultiStatement() ) 
      s.runQuery(new Query("declareUpdate() \n xdmp.documentDelete('" + curURI + "')", "javascript"), null); 
    else 
       s.runQuery(new Query("xdmp.documentDelete('" + curURI + "')", "javascript"), null);
      }
      else{
             s.runQuery(query);
      }
    }
        }
        if (!crudTestData.getRollback()) {
          commitTransaction();
          numDeleted += uris.length;
          for (int p = pos - 1; p >= 0; --p) {
            curURI = uris[p];
            verificationQuery = "fn:count(fn:doc('" + curURI + "'))";
      if(isJSLoad) {
    verifyDeleted( new Query("fn.count(fn.doc('" + curURI + "'))", "javascript"), 0, curURI); 
      } 
      else{
             verifyDeleted(verificationQuery, 0, curURI);
      }
          }
        } else {
          rollbackTransaction();
          // all uris should be there b/c rollback
          for (int p = pos - 1; p >= 0; --p) {
            curURI = uris[p];
            verificationQuery = "fn:count(fn:doc('" + curURI + "'))";
            if(isJSLoad) {
                verifyDeleted( new Query("fn.count(fn.doc('" + curURI + "'))", "javascript"), 1, curURI);
            }
            else{
             verifyDeleted(verificationQuery, 1, curURI);
            }
          }
        }
        } //Changfei: 01/18/2016 solve illegal transaction error in multistatement
        ++i;
        retryCount = 0;

        Thread.sleep(crudTestData.getMaxSleepTime());
        if (numDeleted % crudTestData.getCheckInterval() == 0
            && !crudTestData.getRollback())
          verifyDeleteInterval();
      } catch (RetryableXQueryException e) {
        try {
          rollbackTransaction();
        } catch (Exception Ei) {
        }
        if (++retryCount > 25) {
          System.out.println("Retries exhausted");
          e.printStackTrace();
          ++i;
          retryCount = 0;
        } else {
          System.out.println("Retrying: " + retryCount);
          Thread.sleep(1000);
        }
      } catch (Exception e) {
        e.printStackTrace();
        try {
          rollbackTransaction();
        } catch (Exception Ei) {
        }
        ++i;
        retryCount = 0;
      }
    }

  }

  // METHOD runTest() a virtual method, sub classes must implement
  public void runTest() {
    try {
      System.out.println("Starting test ");
      init();
      System.out.println(crudTestData.toString());
      System.out.println(uniqueURI);
      for (int i = 0; i < crudTestData.getNumOfLoops() && alive; i++) {
        connect();
        loopCnt = i;
        String action = crudTestData.getAction();
        if (action.equalsIgnoreCase("delete")) {
          System.out.println("about to load the content");
          loadContentFromDir(false);
          System.out.println("deleting content");
          if (alive)
            deleteTest();
          if (alive)
            verifyDeleteInterval();
          numLoaded = numLoaded - numDeleted;
          numDeleted = 0;
        } else if (action.equalsIgnoreCase("update")) {
          if (i == 0) {
            System.out.println("about to load the content");
            loadContentFromDir(false);
          }
          System.out.println("updating content");
          if (alive)
            updateTest();
          if (alive)
      if( isJSLoad ){
                verifyUpdateIntervalJS();
            }else{
                verifyUpdateInterval();
            }

        }
        disconnect();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    alive = false;
  }
}
