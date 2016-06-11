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



import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentFactory;


public class
TripleLoadTester
  extends XccLoadTester {
  static int VERIFY_RETRY_MAX = 5;
  private TriplesTestData triplesTestData;
  int numTriplesLoaded = 0;

  public TripleLoadTester(ConnectionData connData, TriplesTestData queryData,
          String threadName) {
  super(connData, queryData.loadTestData, threadName);
  triplesTestData = queryData;
  // triplesTestData.updateGraph(team);
  // triplesTestData.updateSubject(uniqueURI);
  // triplesTestData.updateFirstname(firstName);
  // triplesTestData.updateLastname(lastName);
  triplesTestData.updateQAHome();

  }

  protected void
  loadContentFromDir(boolean rollback)
    throws InterruptedException
  {
    // we need to figure out how to just do one ballplayer here
    
  }

  protected void
  loadTriplesFromDir(boolean rollback)
    throws InterruptedException, Exception
  {
    
    for (SessionHolder s : sessions) {
      String verificationQuery = "QUERY for Count goes here";
      String value = getValueFromResults(s.runQuery(verificationQuery, null));
      numTriplesLoaded = Integer.parseInt(value);
    }
  }

  public void
  runTest() {
    try {
      System.out.println("starting test ");
      System.out.println(loadTestData.toString());
      init();
      System.out.println(uniqueURI);
      for (int i = 0; i < loadTestData.getNumOfLoops() && alive; i++) {
        connect();
        if (isLoadDir) {
          uriDelta = Integer.toString(i);
          loadTriplesFromDir(loadTestData.getRollback());
        } else if (loadTestData.getGenerateQuery().length() != 0) {
          // loadTriplesFromQuery();
        }
        if (alive)
          verifyIntervalAfterIteration(i+1);
        disconnect();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    alive = false;
  }

  protected void
  verifyInterval()
    throws Exception {
  
    if (loadTestData.isInsertTime())
      return;

    int serverIndex = 0;

    for (SessionHolder s : sessions) {
      String xqStr = "verify count query here uniqueURI";

      String value = getValueFromResults(s.runQuery(xqStr, null));
      int numInDB = Integer.parseInt(value);
      if (numInDB != numTriplesLoaded) {
        String error = "ERROR Loaded " + numTriplesLoaded + "triples, graph "
                + uniqueURI + " contained " + numInDB;
        s.runQuery("xdmp:log('" + error + "')");
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      } else {
        if (connData.servers.get(serverIndex).replicas.size() > 0) {
          String timeStamp = getValueFromResults(s.runQuery(
            "xdmp:request-timestamp()", null));
          StressManager.replicaValidationPool.addValidation(new ValidationData(
            connData, xqStr, value, timeStamp, serverIndex));

        }
      }
      if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG"))
        s.runQuery(xqStr);

      serverIndex++;
    }

  }

  private class VerificationTask {
    public String query;

    public VerificationTask(String query) {
      this.query = query;
    }
    public void
    run() {
      try {
        int retryMax = VERIFY_RETRY_MAX;
        if (triplesTestData.loadTestData.isInsertTime())
          retryMax = 120;
        for (SessionHolder s : TripleLoadTester.this.sessions) {
          int retry = 0;
          String value;
          while (true) {
            value = getValueFromResults(s.runQuery(query, null));
            if (value.equalsIgnoreCase("true")) break;
            if (++retry > retryMax) break;
            String error = "RETRY running " + query;
            // s.runQuery("xdmp:log.....");
            System.err.println(error);
            Thread.sleep(5000);
          }
          if (!value.equalsIgnoreCase("true")) {
            String error = "ERROR running " + query;
            // s.runQuery("xdmp:log.....");
            System.err.println(error);
            System.err.println("Exiting...");
            alive = false;
            Thread.sleep(1000);
            System.exit(1);
          }
        }
      } catch (Throwable t) {
        t.printStackTrace();
        String error = "EXCEPTION running " + query;
        System.err.println(error);
        System.err.println("Exiting...");
        alive = false;
      }

    }
  }


}


