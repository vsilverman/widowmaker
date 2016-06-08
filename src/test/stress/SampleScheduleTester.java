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

import java.util.Date;

public class SampleScheduleTester
        extends ScheduleStressTester {

  public static final String TEST_TYPE = "SampleSchedule";

  public SampleScheduleTester() {
  }

  public String getTestType() {
    return TEST_TYPE;
  }

  public TestData getTestDataInstance(String filename)
            throws Exception {

    ScheduleTestData testData = new ScheduleTestData(filename);

    return testData;
  }

  public void initialize(TestData testData, String threadName) {

    System.out.println("SampleScheduleTester.initialize()");

    super.initialize(testData, threadName);

  }

  public void runTest() {

    System.out.println("SampleScheduleTester:  runTest " + new Date());

  }

}

