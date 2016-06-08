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

import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class StressJob
          implements Job {
    protected String configFile;
    protected String threadName;
    protected StressManager stressManager;

  // properties to be fetched off the JobDetail data map
  public static final String TEST_DATA_FILENAME = "quartz.test_data.filename";

  public StressJob() {
  }

  /**
   * used primarily for testing - scheduler will work off class
   * instantiation instead
   */
  public StressJob(String configFile) {
    this.configFile = configFile;
  }

  public void initialize(String configFile, String threadName) {
      this.stressManager = StressManager.getStressManager();
      this.configFile = configFile;
      this.threadName = threadName;
  }

  /**
   * default execution. This instantiates the proper class, creates test data
   * and then sends it on its way
   */
  public void execute(JobExecutionContext context)
                throws JobExecutionException {

    TestData testData = null;
    String testType = null;

    // System.out.println("StressJob.execute");

    JobDataMap dataMap = context.getJobDetail().getJobDataMap();
    String testDataFile = (String)dataMap.get(TEST_DATA_FILENAME);

    try {
      testData = new TestData(testDataFile);
      testType = testData.getTestType();
    } catch (Exception e) {
      System.out.println("Error instantiating test data:");
      e.printStackTrace();
      throw new JobExecutionException("Error instantiating test data");
    }

    try {
    // System.out.println("executing for test type " + testType);

    StressTester tester = StressTest.getStressTester(testType);
    testData = ((LoadableStressTester)tester).getTestDataInstance(testDataFile);

    // System.out.println("initializing test " + testType);

    tester.initialize(testData, "thread1");

    // System.out.println("setting job data " + testType);

    ((SchedulableStressTester)tester).setJobData(dataMap);

    // System.out.println("running test " + testType);

    tester.runTest();

    // System.out.println("completed test " + testType);

    } catch (Exception e) {
      System.out.println("StressJob.execute:  Something very unexpected happened:");
      e.printStackTrace();
    }
  }

}
