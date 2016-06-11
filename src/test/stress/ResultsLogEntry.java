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


public class ResultsLogEntry {
  private String identifier = null;
  private long createTime = 0;
  private long startTime = 0;
  private long finishTime = 0;
  private boolean passed = false;
  private String info = null;

  public ResultsLogEntry clone() {
    ResultsLogEntry newEntry = new ResultsLogEntry();
    newEntry.identifier = this.identifier;
    newEntry.createTime = this.createTime;
    newEntry.startTime = this.startTime;
    newEntry.finishTime = this.finishTime;
    newEntry.passed = this.passed;
    newEntry.info = this.info;

    return newEntry;
  }

  public ResultsLogEntry() {
    createTime = startTime = System.currentTimeMillis();
    identifier = "unknown";
  }

  public ResultsLogEntry(String id) {
    createTime = startTime = System.currentTimeMillis();
    identifier = id;
  }

  public void
  startTimer() {
    startTime = System.currentTimeMillis();
  }

  public void
  stopTimer() {
    finishTime = System.currentTimeMillis();
  }

  public long
  getElapsedRunTime() {
    if (finishTime == 0)
      return 0L;
    else
      return (finishTime - startTime);
  }

  public long
  getElapsedTotalTime() {
    if (finishTime == 0)
      return 0L;
    else
      return (finishTime - createTime);
  }

  public void
  setPassFail(boolean b) {
    passed = b;
  }

  public boolean
  getPassFail() {
    return passed;
  }

  public String
  getPassFailString() {
    if (passed)
      return "PASS";
    else
      return "FAIL";
  }

  public String
  getIdentifier() {
    return identifier;
  }

  public void
  setIdentifier(String id) {
    identifier = id;
  }

  public String
  getInfo() {
    return info;
  }

  public void
  setInfo(String info) {
    this.info = info;
  }
}


