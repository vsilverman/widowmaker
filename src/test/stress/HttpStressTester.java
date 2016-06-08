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

import java.net.URLConnection;
import java.net.HttpURLConnection;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * a basic format for performing HTTP operations without utilising the connection
 * to generate load and log responses
 */
public class HttpStressTester extends XccLoadTester {
    protected ConnectionData connData = null;
    protected HttpTestData httpTestData = null;
    protected String thisUserAgent = DEFAULT_USER_AGENT;
    
    public static final String DEFAULT_USER_AGENT = "WidowMaker";

    public HttpStressTester(ConnectionData connData,
                              HttpTestData testData, String threadName) {
        super(connData, testData.loadTestData, threadName);
        httpTestData = testData;
        this.connData = connData;
        this.testData = testData;
        this.threadName = threadName;
        isPerfTest = testData.getIsPerfTest();
        alive = true;

        setTestName("HttpStressTester URL....");

        System.out.println("HttpStressTester instantiation");
    }

    /**
     * just leaving this here for now to preserve the code and the thinking
     */
    protected void doHttpPost() {

      try {
        HttpURLConnection con = (HttpURLConnection)httpTestData.url.openConnection();

        con.setRequestMethod(httpTestData.method);
        con.setRequestProperty("User-Agent", thisUserAgent);
        con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");

        con.setDoOutput(true);
        DataOutputStream dos = new DataOutputStream(con.getOutputStream());
        dos.writeBytes("my post data goes here");
        dos.flush();
        dos.close();

        int responseCode = con.getResponseCode();

        BufferedReader in = new BufferedReader(
                              new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuilder builder = new StringBuilder();

        while ((inputLine = in.readLine()) != null) {
          builder.append(inputLine);
        }
        in.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    protected void doHttpCall() {
      if (httpTestData.url == null) {
        System.err.println("No URL set up for test data");
        return;
      }

      ResultsLogEntry logEntry = new ResultsLogEntry("thread " + threadName +
                                                      " HttpRequest");
      logEntry.startTimer();

      int responseCode = 0;

      try {
      HttpURLConnection con = (HttpURLConnection)httpTestData.url.openConnection();

      con.setRequestMethod(httpTestData.method);

      con.setRequestProperty("User-Agent", thisUserAgent);

      responseCode = con.getResponseCode();

      BufferedReader in = new BufferedReader(
                                new InputStreamReader(con.getInputStream()));
      StringBuilder builder = new StringBuilder();
      String inputLine;

      while ((inputLine = in.readLine()) != null) {
        builder.append(inputLine);
      }
      in.close();

      } catch (IOException e) {
      }

      if (responseCode == 200)
        logEntry.setPassFail(true);
      else
        logEntry.setPassFail(false);

      logEntry.stopTimer();

      ResultsLogger logger =
        StressManager.getResultsLogger(httpTestData.getLogFileName());
      logger.logResult(logEntry);

    }

    // METHOD runTest() a virtual method, sub classes must implement
    public void runTest() {

      int loops = testData.getNumOfLoops();
      int count = 1000;

      System.out.println("HttpStressTester:  loops is " + loops);

      try {
        init();
        for (int jj = 0; jj < loops && alive; jj++) {
          for (int ii = 0; ii < count && alive; ii++) {
            connect();

            updateCounter(ii);

            doHttpCall();

            if (alive)
              verifyIntervalAfterIteration(ii+1);

            disconnect();

          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

      ResultsLogger logger =
        StressManager.getResultsLogger(httpTestData.getLogFileName());
      logger.logMessage("thread " + threadName + " complete");
    }

    protected void verifyInterval(int ct) {
    }

    protected void verifyIntervalAfterIteration(int ct) {
    }

    // TODO:  might this open the connection?

    public void connect() throws Exception {
        // no-op;
    }

    public void disconnect() {
        // no-op;
    }

    public void setTransactionTimeout(int t) throws Exception {
        // no-op;
    }
}
