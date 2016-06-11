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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.marklogic.xcc.exceptions.RetryableXQueryException;

public class RestCRUDTester extends RestLoadTester {
  // include data class here
  protected CRUDTestData crudTestData = null;
  protected int numDeleted     = 0;
  protected int numUpdated     = 0;
  protected int numPatched     = 0;
  protected int numTransformed = 0;
  protected int loopCnt        = 0;
  protected String[] miscStr = { "Aggregate", "Enable", "Leverage",
      "Facilitate", "Synergize", "Repurpose", "Strategize", "Reinvent",
      "Harness", "cross-platform", "best-of-breed", "frictionless",
      "ubiquitous", "extensible", "compelling", "mission-critical",
      "collaborative", "integrated", "methodologies", "infomediaries",
      "platforms", "schemas", "mindshare", "paradigms", "functionalities",
      "web services", "infrastructures" };
  protected String getTransformName = "get-transform-ele";
  protected String putTransformName = "put-transform-ele";
  protected Map<String, List<String>> getTransform;
  protected Map<String, List<String>> putTransform;

  public RestCRUDTester(ConnectionData connData, CRUDTestData crudData,
                        String threadName) {
    super(connData, (LoadTestData) crudData, threadName);
    crudTestData = crudData;

    Map<String, List<String>> transformParams;

    transformParams = new HashMap<String, List<String>>();
    transformParams.put("name", Collections.singletonList(getTransformName));
    getTransform = session.makeTransform("stamp", transformParams);

    transformParams = new HashMap<String, List<String>>();
    transformParams.put("name", Collections.singletonList(putTransformName));
    putTransform = session.makeTransform("stamp", transformParams);
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
    // message("numStrings " + numStrings + " retStr ");
    return retStr;
  }

  private void updateURI(String URI) {
      //message("Updating: " + URI);
      long startTime = System.currentTimeMillis();
      String doc = session.getDocument(URI, null);
      int lastClose = doc.lastIndexOf("</");
      String tag = doc.substring(lastClose);
      doc = doc.substring(0, lastClose) + "<update-ele>" + getContent() + "</update-ele>" + tag;
      session.putDocument(URI, doc.getBytes(), null);
      totalTime += System.currentTimeMillis() - startTime;
      //message("Updated in " + (System.currentTimeMillis() - startTime) + ": " + URI);
  }

  protected void verifyUpdated(int expected, String curURI) throws Exception {
      //message("About to verify: " + curURI);

      // This is a stress test, get the doc even if we're going to use an extension resource
      // to compute the actual value we need.
      String doc = session.getDocument(curURI, null);
      if (doc == null) {
          // This can't really happen and I don't really care.
      }

      String value = session.getResource("getElements", "text/plain",
    		  "doc", curURI, "child-element", "update-ele");
      int numInDB = Integer.parseInt(value);

      //message("Verified: " + value + " " + numInDB + ": " + curURI);

      if (expected != numInDB) {
        String error = "ERROR number of elements after update FAIL expected "
            + expected + " found " + numInDB + " URI " + curURI;
        //s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      }
  }

  protected void verifyUpdateInterval() throws Exception {
      String value = session.getResource("getElements", "text/plain",
    		  "collection", uniqueURI, "child-element", "update-ele"
    		  );
      int numInDB = Integer.parseInt(value);
      if (numInDB != numUpdated) {
        String error = "ERROR update test failed expected " + numUpdated
            + " from  collection " + uniqueURI + " \n db contained " + numInDB;
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      }
  }

  private void patchURI(String URI) {
      //message("Patching: " + URI);
      long startTime = System.currentTimeMillis();

      String patch =
"<rapi:patch xmlns:rapi=\"http://marklogic.com/rest-api\">"+
"<rapi:insert context=\"/*\" position=\"last-child\">"+
    "<patch-ele>" + getContent() + "</patch-ele>"+
"</rapi:insert>"+
"</rapi:patch>";
      session.patch(URI, patch.getBytes(), null);
      totalTime += System.currentTimeMillis() - startTime;
      //message("Patched in " + (System.currentTimeMillis() - startTime) + ": " + URI);
  }

  protected void verifyPatched(int expected, String curURI)
  throws Exception {
      //message("About to verify: " + curURI);

      // This is a stress test, get the doc even if we're going to use an extension resource
      // to compute the actual value we need.
      String doc = session.getDocument(curURI, null);
      if (doc == null) {
          // This can't really happen and I don't really care.
      }

      String value = session.getResource("getElements", "text/plain",
    		  "doc", curURI, "child-element", "patch-ele"
    		  );
      int numInDB = Integer.parseInt(value);

      //message("Verified: " + value + " " + numInDB + ": " + curURI);

      if (expected != numInDB) {
        String error = "ERROR number of elements after patch FAIL expected "
            + expected + " found " + numInDB + " URI " + curURI;
        //s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      }
  }

  protected void verifyPatchInterval() throws Exception {
      String value = session.getResource("getElements", "text/plain",
    		  "collection", uniqueURI, "child-element", "patch-ele");
      int numInDB = Integer.parseInt(value);
      if (numInDB != numPatched) {
        String error = "ERROR patch test failed expected " + numPatched
            + " from  collection " + uniqueURI + " \n db contained " + numInDB;
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      }
  }

  private void transformURI(String URI) {
      //message("Transforming: " + URI);
      long startTime = System.currentTimeMillis();
      String doc = session.getDocument(URI, null);
      putTransform.put("value", Collections.singletonList(getContent()));
      session.putDocument(URI, doc.getBytes(), putTransform, null);
      totalTime += System.currentTimeMillis() - startTime;
      //message("Transformed in " + (System.currentTimeMillis() - startTime) + ": " + URI);
  }

  protected void verifyTransformed(int expected, String curURI)
  throws Exception {
      //message("About to verify: " + curURI);

      getTransform.put("value", Collections.singletonList(getContent()));
      String doc = session.getDocument(curURI, getTransform, null);
      if (doc == null) {
          String error = "ERROR get transform FAILED on URI " + curURI;
          //s.runQuery("xdmp:log('" + error + "')");
          System.err.println(error);
          System.err.println("Exiting...");
          alive = false;
      }

      int tagClose = doc.lastIndexOf("</", doc.lastIndexOf("</") - 1);
      String tagOpen = doc.substring(tagClose + 2);
      String tagName = tagOpen.substring(0,tagOpen.indexOf(">")).trim();
      if (!getTransformName.equals(tagName)) {
          String error = "ERROR get transform FAILED for element on URI " + curURI;
          System.err.println(error);
          System.err.println("Exiting...");
          alive = false;
      }

      String value = session.getResource("getElements", "text/plain",
    		  "doc", curURI, "child-element", putTransformName
    		  );
      int numInDB = Integer.parseInt(value);

      //message("Verified: " + value + " " + numInDB + ": " + curURI);

      if (expected != numInDB) {
        String error = "ERROR number of elements after put transform FAIL expected "
            + expected + " found " + numInDB + " URI " + curURI;
        //s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      }
  }

  protected void verifyTransformInterval() throws Exception {
      String value = session.getResource("getElements", "text/plain",
    		  "collection", uniqueURI,  "child-element", putTransformName);
      int numInDB = Integer.parseInt(value);
      if (numInDB != numTransformed) {
        String error = "ERROR transform test failed expected " + numTransformed
            + " from  collection " + uniqueURI + " \n db contained " + numInDB;
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      }
  }

  protected void verifyDeleted(int numDeleted, String[] uris) throws Exception {
      // if we didn't delete any, then we expect to find them
      boolean expected = (numDeleted == 0) ? true : false;
      for (String curURI : uris) {
          boolean value = session.docExists(curURI, null);
          if (value != expected) {
            String error = "ERROR found URI after delete : " + curURI
                + " exists? " + value + " expected? " + expected;
            System.err.println(error);
            System.err.println("Exiting...");
            alive = false;
            break;
          }
      }
  }

  /** @return number of documents deleted */
  protected int finishBatch(String[] uris, boolean rollback, Object transaction) throws Exception {
      if (!rollback) {
          commitRestTransaction(transaction);
          return uris.length;
      } else {
          rollbackRestTransaction(transaction);
          return 0; // none deleted
      }
  }

  protected void verifyDeleteInterval(String phase) throws Exception {
      String value = session.getResource("getDocs", "text/plain", "collection", uniqueURI);
      int numInDB = Integer.parseInt(value);
      if (numInDB != numLoaded - numDeleted) {
        String error = "ERROR delete test ";
        if (phase != null)
        	error = error+phase+" ";
        error = error+"failed expected "
            + (numLoaded - numDeleted) + " from  collection " + uniqueURI
            + " contained " + numInDB;
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      }
      /* ???
      if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG")) {
        s.runQuery(xqStr);
      }
      */
  }

  String[] getURIS(int loopUpdateCount) throws Exception {
    int batchSize = 10;
    if (crudTestData.getMultiStatement()) {
      batchSize = crudTestData.getBatchSize();
    }

      String uridoc = session.getResource("getURIs", "text/plain",
              "collection", uniqueURI,
              "from", ""+((loopUpdateCount * batchSize) + 1),
              "to", ""+((loopUpdateCount * batchSize) + batchSize));

      String[] xuris = uridoc.split("\n");
      String[] uris = new String[xuris.length - 1];

      if (xuris.length > 1) {
          System.arraycopy(xuris, 1, uris, 0, uris.length);
      }

    return uris;
  }

  public void updateTest() throws Exception {
      int retryCount = 0;
      int i = 0;
      while (i < numLoaded && alive) {
          //message("Updating " + i + " of " + numLoaded + ": " + alive);
          Object transaction = beginRestTransaction();
          try {
              String[] uris = getURIS(i);
              // if(uris.length == 0) i = numLoaded;
              int pos = 0;
              for (; pos < uris.length && alive; pos++) {
                  updateURI(uris[pos]);
              }
              if (!crudTestData.getRollback()) {
                  commitRestTransaction(transaction);
                  numUpdated += uris.length;
                  for (int p = pos - 1; p >= 0; --p)
                      verifyUpdated(loopCnt + 1, uris[p]);
              } else {
                  rollbackRestTransaction(transaction);
                  for (int p = pos - 1; p >= 0; --p)
                      verifyUpdated(0, uris[p]);
              }
              ++i;
              retryCount = 0;

              Thread.sleep(crudTestData.getMaxSleepTime());
              if (numUpdated % crudTestData.getCheckInterval() == 0
                      && !crudTestData.getRollback())
                  verifyUpdateInterval();
          } catch (RetryableXQueryException e) {
              try {
                  rollbackRestTransaction(transaction);
              } catch (Exception Ei) {
              }
              if (++retryCount > 25) {
                  message("Retries exhausted");
                  e.printStackTrace();
                  ++i;
                  retryCount = 0;
              } else {
                  message("Retrying: " + retryCount);
                  Thread.sleep(1000);
              }
          } catch (Exception e) {
              e.printStackTrace();
              try {
                  rollbackRestTransaction(transaction);
              } catch (Exception Ei) {
              }
              ++i;
              retryCount = 0;
          }
      }
  }

  public void patchTest() throws Exception {
      int retryCount = 0;
      int i = 0;
      while (i < numLoaded && alive) {
          //message("Patching " + i + " of " + numLoaded + ": " + alive);
          Object transaction = beginRestTransaction();
          try {
              String[] uris = getURIS(i);
              // if(uris.length == 0) i = numLoaded;
              int pos = 0;
              for (; pos < uris.length && alive; pos++) {
                  patchURI(uris[pos]);
              }
              if (!crudTestData.getRollback()) {
                  commitRestTransaction(transaction);
                  numPatched += uris.length;
                  for (int p = pos - 1; p >= 0; --p)
                      verifyPatched(loopCnt + 1, uris[p]);
              } else {
                  rollbackRestTransaction(transaction);
                  for (int p = pos - 1; p >= 0; --p)
                      verifyPatched(0, uris[p]);
              }
              ++i;
              retryCount = 0;

              Thread.sleep(crudTestData.getMaxSleepTime());
              if (numPatched % crudTestData.getCheckInterval() == 0
                      && !crudTestData.getRollback())
                  verifyPatchInterval();
          } catch (RetryableXQueryException e) {
              try {
                  rollbackRestTransaction(transaction);
              } catch (Exception Ei) {
              }
              if (++retryCount > 25) {
                  message("Retries exhausted");
                  e.printStackTrace();
                  ++i;
                  retryCount = 0;
              } else {
                  message("Retrying: " + retryCount);
                  Thread.sleep(1000);
              }
          } catch (Exception e) {
              e.printStackTrace();
              try {
                  rollbackRestTransaction(transaction);
              } catch (Exception Ei) {
              }
              ++i;
              retryCount = 0;
          }
      }
  }

  public void transformTest() throws Exception {
      int retryCount = 0;
      int i = 0;
      while (i < numLoaded && alive) {
          //message("Transforming " + i + " of " + numLoaded + ": " + alive);
          Object transaction = beginRestTransaction();
          try {
              String[] uris = getURIS(i);
              // if(uris.length == 0) i = numLoaded;
              int pos = 0;
              for (; pos < uris.length && alive; pos++) {
            	  transformURI(uris[pos]);
              }
              if (!crudTestData.getRollback()) {
                  commitRestTransaction(transaction);
                  numTransformed += uris.length;
                  for (int p = pos - 1; p >= 0; --p)
                      verifyTransformed(loopCnt + 1, uris[p]);
              } else {
                  rollbackRestTransaction(transaction);
                  for (int p = pos - 1; p >= 0; --p)
                      verifyTransformed(0, uris[p]);
              }
              ++i;
              retryCount = 0;

              Thread.sleep(crudTestData.getMaxSleepTime());
              if (numTransformed % crudTestData.getCheckInterval() == 0
                      && !crudTestData.getRollback())
                  verifyTransformInterval();
          } catch (RetryableXQueryException e) {
              try {
                  rollbackRestTransaction(transaction);
              } catch (Exception Ei) {
              }
              if (++retryCount > 25) {
                  message("Retries exhausted");
                  e.printStackTrace();
                  ++i;
                  retryCount = 0;
              } else {
                  message("Retrying: " + retryCount);
                  Thread.sleep(1000);
              }
          } catch (Exception e) {
              e.printStackTrace();
              try {
                  rollbackRestTransaction(transaction);
              } catch (Exception Ei) {
              }
              ++i;
              retryCount = 0;
          }
      }
  }

  protected void deleteOne(String uri, Object transaction) {
      session.delete(uri, transaction);
  }

  public void deleteTest() throws Exception {
    int retryCount = 0;
    String curURI;
    int i = 0;
      message("Deleting " + numLoaded + " documents.");
    while (i < numLoaded && alive) {
      Object transaction = beginRestTransaction();
      try {
        // delete uri here
        String[] uris = getURIS(0);
        int pos = 0;
        for (; pos < uris.length && alive; pos++) {
          curURI = uris[pos];
            //message("Deleting " + (pos+1) + " " + curURI);
            deleteOne(curURI, transaction);
        }
        int numDeletedInBatch = finishBatch(uris, crudTestData.getRollback(), transaction);
        numDeleted += numDeletedInBatch;
        verifyDeleted(numDeletedInBatch, uris);
        ++i;
        retryCount = 0;

        Thread.sleep(crudTestData.getMaxSleepTime());
        if (numDeleted % crudTestData.getCheckInterval() == 0
            && !crudTestData.getRollback())
          verifyDeleteInterval("transaction");
      } catch (RetryableXQueryException e) {
        try {
          rollbackRestTransaction(transaction);
        } catch (Exception Ei) {
        }
        if (++retryCount > 25) {
          message("Retries exhausted");
          e.printStackTrace();
          ++i;
          retryCount = 0;
        } else {
          message("Retrying: " + retryCount);
          Thread.sleep(1000);
        }
      } catch (Exception e) {
        e.printStackTrace();
        try {
          rollbackRestTransaction(transaction);
        } catch (Exception Ei) {
        }
        ++i;
        retryCount = 0;
      }
    }

  }

  public void runTest() {
      try {
          startRun();
          init();
          message("========\n" +
              "Starting test with uniqueURI = " + uniqueURI + "\n" +
              crudTestData.toString() + "\n" +
              "========");
          for (int i = 0; i < crudTestData.getNumOfLoops() && alive; i++) {
              //message("Connecting for loop " + i + " of " + crudTestData.getNumOfLoops() + ": " + alive);
              connect();
              loopCnt = i;
              String action = crudTestData.getAction();
              if ("delete".equalsIgnoreCase(action)) {
                  message("about to load the content");
                  loadContentFromDir(false);
                  if (alive) {
                      message("deleting content");
                      deleteTest();
                  }
                  if (alive) {
                      message("verifying delete interval");
                      verifyDeleteInterval("summary");
                  }
                  numLoaded = numLoaded - numDeleted;
                  numDeleted = 0;
              } else if ("update".equalsIgnoreCase(action)) {
                  if (i == 0) {
                      message("about to load the content");
                      loadContentFromDir(false);
                  }
                  if (alive) {
                      message("updating content");
                      updateTest();
                  }
                  if (alive) {
                      message("verifying update interval");
                      verifyUpdateInterval();
                  }
              } else if ("patch".equalsIgnoreCase(action)) {
                  if (i == 0) {
                      message("about to load the content");
                      loadContentFromDir(false);
                  }
                  if (alive) {
                      message("patching content");
                      patchTest();
                  }
                  if (alive) {
                      message("verifying patch interval");
                      verifyPatchInterval();
                  }
              } else if ("transform".equalsIgnoreCase(action)) {
                  if (i == 0) {
                      message("about to load the content");
                      loadContentFromDir(false);
                  }
                  if (alive) {
                      message("transforming content");
                      transformTest();
                  }
                  if (alive) {
                      message("verifying transform interval");
                      verifyTransformInterval();
                  }
              }
              disconnect();
          }
      } catch (Exception e) {
          e.printStackTrace();
      }
      alive = false;
      endRun();
  }
}
