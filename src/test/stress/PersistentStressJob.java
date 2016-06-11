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

import org.joda.time.DateTime;

import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.PersistJobDataAfterExecution;

/**
 * Boilerplate for a job that passes data from run to run
 */
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class PersistentStressJob
          extends StressJob {
    protected String configFile;
    protected String threadName;

    public static final boolean debugFlag = false;

    public static final String LAST_TIME_RUN = "lastRun";
    public static final String FIRST_TIME_RUN = "firstRun";

  public PersistentStressJob() {
  }

  /**
   * used primarily for testing - scheduler will work off class
   * instantiation instead
   */
  public PersistentStressJob(String configFile) {
    super(configFile);
  }

  /**
   * default execution. This instantiates the proper class, creates test data
   * and then sends it on its way
   */
  public void execute(JobExecutionContext context)
                throws JobExecutionException {

    
    JobKey jobKey = context.getJobDetail().getKey();

    JobDataMap dataMap = context.getJobDetail().getJobDataMap();
    DateTime dateTime = (DateTime)dataMap.get(LAST_TIME_RUN);

    if (dateTime == null) {
      System.out.println("PersistentStressJob:  " + jobKey + " running for first time");
      dataMap.put(FIRST_TIME_RUN, dateTime);
    } else {
      if (debugFlag)
        System.out.println("PersistentStressJob:  " + jobKey + " last executed at " + dateTime);
    }

/*
    TestData testData = null;
    String testType = null;
    try {
      testData = new TestData(configFile);
      testType = testData.getTestType();
    } catch (Exception e) {
      System.out.println("Error instantiating test data:");
      e.printStackTrace();
      throw new JobExecutionException("Error instantiating test data");
    }

    StressTester tester = StressTest.getStressTester(testType);

    tester.runTest();
*/

    super.execute(context);

    dateTime = new DateTime();
    dataMap.put(LAST_TIME_RUN, dateTime);

  }

}
