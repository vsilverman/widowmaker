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

import java.util.Date;
import java.util.Iterator;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Duration;

import test.stress.ScheduleStressTester;
import test.stress.PersistentScheduleStressTester;
import test.stress.TestData;
import test.stress.Query;
import test.stress.StressTestProperties;
import test.stress.ResultsLogEntry;
import test.stress.ResultsLogger;
import test.stress.StressManager;

public class SqlPropModTester
        extends PersistentScheduleStressTester {

  public static final boolean debugFlag = false;
  public static final String TEST_TYPE = "SqlPropertyModification";

  public SqlPropModTester() {
  }

  public String getTestType() {
    return TEST_TYPE;
  }

  public TestData getTestDataInstance(String filename)
            throws Exception {

    // System.out.println("SqlPropModTester.getTestDataInstance for file " + filename);

    SqlPropModTestData testData = new SqlPropModTestData(filename);

    return testData;
  }

  public void initialize(TestData testData, String threadName) {

    // System.out.println("SqlPropModTester.initialize()");

    if (!(testData instanceof SqlPropModTestData)) {
      System.out.println("SqlPropModTester - test data is not SqlPropModTestData");
    }

    super.initialize(testData, threadName);

  }

  protected void runOneQuery(Query query) {

  }

  public static final String LAST_TIME_SET = "sql.stress.PropMod.LastTimeSet";
  public static final String LAST_TIME_SET_SUFFIX = ".lastTimeSet";

  public void runTest() {

    // System.out.println("SqlPropModTester:  runTest " + new Date());

    SqlPropModTestData sqlTestData = (SqlPropModTestData)this.testData;

    StressTestProperties stressProperties = StressTestProperties.getStressTestProperties();

    Iterator propertyNames = sqlTestData.propertyNames();

    DateTime lastTimeSet = (DateTime)dataMap.get(LAST_TIME_SET);
    if (lastTimeSet == null) {
      if (debugFlag)
        System.out.println("lastTimeSet is null");
    } else {
      if (debugFlag)
        System.out.println("lastTimeSet was set to " + lastTimeSet);
    }

    DateTime currentTime = new DateTime();

    while (propertyNames.hasNext()) {
      String name = (String)propertyNames.next();
      // System.out.println("processing property " + name);
      String propValue = stressProperties.getProperty(name);
      // System.out.println("current value:  " + propValue);
      int value = 0;
      if (propValue != null) {
        value = Integer.parseInt(propValue);
      }
      int intervalSecs = sqlTestData.getInterval(name);
      int increment = sqlTestData.getIncrement(name);
      String lastSetProp = name + LAST_TIME_SET_SUFFIX;
      DateTime propLastTimeSet = (DateTime)dataMap.get(lastSetProp);
      // System.out.println("prop, lastTimeSet: " + name + ", " + propLastTimeSet);

      if (propLastTimeSet != null) {
        DateTime nowTime;
        nowTime = new DateTime(System.currentTimeMillis());
        Interval interval = new Interval(propLastTimeSet, nowTime);
        Duration duration = interval.toDuration();
        long secs = duration.getStandardSeconds();
        // System.out.println("duration in secs is " + secs);

        if (secs < intervalSecs) {
          // System.out.println("intervalSecs is " + intervalSecs + ", skipping");
        } else {
          // System.out.println("intervalSecs is " + intervalSecs + ", processing");
          // System.out.println("increment is " + increment);
          value += increment;
          // System.out.println("new value is " + value);
          stressProperties.setProperty(name, Integer.toString(value));
          dataMap.put(lastSetProp, currentTime);

          System.out.println("SqlPropModTester:  Setting property " + name + " to " + value);

          ResultsLogEntry logEntry = new ResultsLogEntry(testData.getTestType() 
                                          + " Property " + name
                                          + " set to " + value);
          logEntry.stopTimer();
          logEntry.setPassFail(true);
          ResultsLogger logger = StressManager.getResultsLogger(testData.getLogFileName());
          if (logger != null)
            logger.logResult(logEntry);
        }
      } else {
        dataMap.put(lastSetProp, currentTime);
      }

      
    }

    dataMap.put(LAST_TIME_SET, new DateTime());
  }

}

