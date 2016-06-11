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

import java.io.Reader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

import com.marklogic.xcc.ResultSequence;

public abstract class StressTester {
  protected TestData testData = null;
  protected String threadName = null;
  protected boolean alive = true;
  protected double totalTime = 0;
  protected String uniqueURI = null;
  protected ArrayList<String> responseTimes = new ArrayList<String>();
  protected boolean isPerfTest = false;
  protected Calendar timer = null;

  public static final int DEFAULT_MAX_SLEEP_TIME = 500;

  protected static Random randomSleeper = new Random();

  public StressTester() {
      // nop;
  }

  /**
   * Returns an instance of the test data class that this particular
   * test uses. This is not necessary for the provided classes, but is
   * the way to allow a widowmaker extension to be classloaded and
   * determine its own testdata
   */
/*
  public static TestData getTestDataInstance(String configFilename)
      throws Exception {
    return new TestData(configFilename);
  }
*/

  /**
   * Returns the string that is the test type for this particular test.
   * This is used to tie the testtype in the configuration file to
   * this class for dynamic class loading. This should be overridden.
   */
/*
  public String getTestType() {
    return "unknown";
  }
*/

  public void initialize(TestData testData, String threadName) {

    this.testData = testData;
    this.threadName = threadName;
  }

  public void setAlive(boolean alive) {
    this.alive = alive;
  }

  public boolean isAlive() {
    return alive;
  }

  public void message(String msg) {
      System.out.println(getTimeString() +
          " (Thread=" + Thread.currentThread().getId() + ") " + msg);
  }

    public void startRun() {
        timer = new GregorianCalendar();
    }

    public void endRun() {
        Calendar now = new GregorianCalendar();
        long diff = now.getTimeInMillis() - timer.getTimeInMillis();
        message("Test took: " + diff + "ms");
    }

    /**
     * Call to sleep between iterations while performing operations in a loop
     * Sleeps a random period of time whose maximum value is the period of time
     * set as sleeptime in the test data file.
     * If no time was specified, a default sleep (currently 500 milliseconds)
     * is used.
     */
    public void sleepBetweenIterations()
      throws InterruptedException {
      int maxSleep = testData.getMaxSleepTime();
      if (maxSleep == 0)
        maxSleep = DEFAULT_MAX_SLEEP_TIME;
      sleepBetweenIterations(maxSleep);
    }

    /**
     * Call to sleep between iterations for a random amount of time whose maximum
     * value is passed in.
     *
     * @maxSleepTime the maximum time the thread will sleep
     */
    public void sleepBetweenIterations(long maxSleepTime)
      throws InterruptedException {
      long maxVal = maxSleepTime;
      long sleepTime = -1;

      if (maxVal < 1) {
        maxVal = 1;
        sleepTime = 1;
      }

      while (sleepTime < 0) {
        sleepTime = randomSleeper.nextLong() % maxVal;
      }

      Thread.sleep(sleepTime);
    }

    public abstract void runTest();
  public abstract void connect() throws Exception;
  public abstract void disconnect();
  public abstract void beginTransaction() throws Exception;
  public abstract void commitTransaction() throws Exception;

  public void accumulateResponse() {
    responseTimes.add(createTimeElement());
  }

  public String getResponseElements(boolean update) {
    StringBuffer sb = new StringBuffer();
    if (update)
      sb.append("(");
    for (int i = 0; i < responseTimes.size(); i++) {
      sb.append((String) responseTimes.get(i));
      if (update && i + 1 < responseTimes.size())
        sb.append(",");
    }
    if (update)
      sb.append(")");
    return sb.toString();
  }

  public void addOrUpdateResponseDoc() {
    String timeDocURI = "time-doc.xml";
    String xqStr = "xquery version '1.0-ml';" + " if(exists(doc('" + timeDocURI
        + "')))" + "   then  xdmp:node-insert-child( doc('" + timeDocURI
        + "')/* , " + getResponseElements(true) + ")"
        + "   else xdmp:document-insert( '" + timeDocURI + "', <times> "
        + getResponseElements(false) + "</times>)";
    responseTimes.clear();
  }

  public String createTimeElement() {
    boolean isUpdate = false;
    if (testData instanceof CRUDTestData)
      if (((CRUDTestData) testData).getAction().equalsIgnoreCase("update"))
        isUpdate = true;
    String element = null;
    if (!isUpdate)
      element = "<elapsed-time batch-size='" + testData.getBatchSize()
          + "' update='false'>" + ((int) totalTime) + "</elapsed-time>";
    else
      element = "<elapsed-time batch-size='" + testData.getBatchSize()
          + "' update='true'>" + ((int) totalTime) + "</elapsed-time>";
    return element;

  }

  public abstract void rollbackTransaction() throws Exception;
  public abstract void setTransactionTimeout(int t) throws Exception;

  public static String randomString(int length) {
    String s = "";
    for (int i = 0; i < length; i++) {
      // ASCII A-Z == 41hex-5Ahex
      int m = 65 + (int) (26 * (Math.random()));
      s += new Character((char) m);
    }
    return s;
  }

  public static String getDateString() {
    Calendar calendar = new GregorianCalendar();
    String dateStr = Integer.toString(calendar.get(Calendar.YEAR))
        + Integer.toString(calendar.get(Calendar.MONTH))
        + Integer.toString(calendar.get(Calendar.DAY_OF_MONTH))
        + Integer.toString(calendar.get(Calendar.HOUR_OF_DAY))
        + Integer.toString(calendar.get(Calendar.MINUTE))
        + Integer.toString(calendar.get(Calendar.SECOND))
        + Integer.toString(calendar.get(Calendar.MILLISECOND));
    return dateStr;
  }

  public static String getTimeString() {
      Calendar calendar = new GregorianCalendar();
      int y = calendar.get(Calendar.YEAR);
      int mo = calendar.get(Calendar.MONTH) + 1;
      int d = calendar.get(Calendar.DAY_OF_MONTH);
      int h = calendar.get(Calendar.HOUR_OF_DAY);
      int m = calendar.get(Calendar.MINUTE);
      int s = calendar.get(Calendar.SECOND);
      String timeStr = y + "-"
              + ((mo < 10) ? "0" : "")
              + mo + "-"
              + ((d < 10) ? "0" : "")
              + d + " "
              + ((h < 10) ? "0" : "")
              + h + ":"
              + ((m < 10) ? "0" : "")
              + m + ":"
              + ((s < 10) ? "0" : "")
              + s;
      return timeStr;
  }

  public static String getValueFromResults(ResultSequence resultSeq) {
    StringBuffer resultsBuffer = new StringBuffer();
    try {
      char[] readBuffer = new char[32 * 1024];
      while (resultSeq.hasNext()) {
        Reader buf = resultSeq.next().asReader();
        while (true) {
          int actual = buf.read(readBuffer);
          if (actual <= 0)
            break;
          resultsBuffer.append(readBuffer, 0, actual);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        resultSeq.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return resultsBuffer.toString().trim();
  }
}
