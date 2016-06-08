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

import org.quartz.Trigger;
import org.quartz.JobDetail;
import org.quartz.JobDataMap;


public interface SchedulableStressTester {

  public JobDetail makeJobDetail(String inputFile);

  public Trigger makeTrigger(TestData testData);

  public void initialize(TestData testData, String threadName);

  public void setJobData(JobDataMap dataMap);

}

