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


package test.sql;

import java.util.Date;
import java.util.Iterator;

import test.stress.ScheduleStressTester;
import test.stress.TestData;
import test.stress.Query;

public class SqlMonitorScheduleTester
        extends ScheduleStressTester {

  public static final String TEST_TYPE = "SqlMonitorCounts";

  public SqlMonitorScheduleTester() {
  }

  public String getTestType() {
    return TEST_TYPE;
  }

  public TestData getTestDataInstance(String filename)
            throws Exception {

    System.out.println("SqlMonitorScheduleTester.getTestDataInstance for file " + filename);

    SqlMonitorTestData testData = new SqlMonitorTestData(filename);

    return testData;
  }

  public void initialize(TestData testData, String threadName) {

    System.out.println("SqlMonitorScheduleTester.initialize()");

    if (!(testData instanceof SqlMonitorTestData)) {
      System.out.println("SqlMonitorScheduleTester - test data is not SqlMonitorTestData");
    }

    super.initialize(testData, threadName);

  }

  protected void runOneQuery(Query query) {

  }

  public void runTest() {

    System.out.println("SqlMonitorScheduleTester:  runTest " + new Date());

    SqlMonitorTestData sqlTestData = (SqlMonitorTestData)this.testData;

    System.out.println("total queries:  " + sqlTestData.getQueryCount());

    Iterator queries = sqlTestData.getQueries();

    int count = 0;
    while (queries.hasNext()) {
      Query query = (Query)queries.next();
      System.out.println("query " + count++ + ":");
      System.out.println(query.toString());
      runOneQuery(query);
    }

  }

}

