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

import org.quartz.Trigger;
import org.quartz.SimpleTrigger;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobDataMap;
import org.quartz.DateBuilder;
import org.quartz.TriggerBuilder;
import org.quartz.ScheduleBuilder;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.CronScheduleBuilder;
import org.quartz.SchedulerException;

public abstract class PersistentScheduleStressTester extends ScheduleStressTester
    implements LoadableStressTester,
                SchedulableStressTester {

    public PersistentScheduleStressTester(ScheduleTestData testData, String threadName) {
      super(testData, threadName);
    }

    public PersistentScheduleStressTester() {
    }

    public void initializeTester(ScheduleTestData testData, String threadName) {
      super.initialize(testData, threadName);
    }

/*
    // METHOD runTest() a virtual method, sub classes must implement
    public abstract void runTest();

    public void connect() throws Exception {
        // no-op;
    }

    public void disconnect() {
        // no-op;
    }

    public void setTransactionTimeout(int t) throws Exception {
        // no-op;
    }

  public void beginTransaction()
          throws Exception {

  }

  public void commitTransaction()
          throws Exception {

  }

  public void rollbackTransaction()
          throws Exception {

  }
*/

  public JobDetail makeJobDetail(String inputFile) {

    if (testData == null)
      return null;

    String testType = testData.getTestType();
    System.out.println("making jobBuilder with test type " + testType);

    JobBuilder jobBuilder = JobBuilder.newJob(PersistentStressJob.class);
    jobBuilder.withIdentity(testType, "PersistentStressGroup");
    JobDetail detail = jobBuilder.build();
    JobDataMap dataMap = detail.getJobDataMap();
    dataMap.put(StressJob.TEST_DATA_FILENAME, inputFile);

    return detail;
  }

/*
  public Trigger makeTrigger(TestData testData) {

    if (testData == null)
      return null;

    ScheduleTestData schedTestData = (ScheduleTestData)testData;

    Date startTime = DateBuilder.nextGivenMinuteDate(null, 1);

    // interval tests take precedence
    int interval = schedTestData.getInterval();
    if (interval > 0) {
      TriggerBuilder builder = TriggerBuilder.newTrigger();
      builder.withIdentity("blah-" + schedTestData.getTestType(), "StressGroup");
      builder.startAt(startTime);
      SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder.simpleSchedule();
      scheduleBuilder.withIntervalInSeconds(interval);
      scheduleBuilder.repeatForever();
      builder.withSchedule(scheduleBuilder);
      SimpleTrigger trigger = (SimpleTrigger)builder.build();
      return trigger;
    }
    String cronString = schedTestData.getCronString();
    if (cronString != null) {
      TriggerBuilder builder = TriggerBuilder.newTrigger();
      builder.withIdentity("blah-" + schedTestData.getTestType(), "StressGroup");
      builder.withSchedule(CronScheduleBuilder.cronSchedule(cronString));
      CronTrigger trigger = (CronTrigger)builder.build();
      return trigger;
    }

    return null;
  }
*/

}
