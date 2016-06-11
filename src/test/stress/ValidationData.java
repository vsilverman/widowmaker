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


public class ValidationData {
  private ConnectionData connData = null;
  private String validationQuery = null;
  private String expectedValue = null;
  private String tStamp = null;
  private int svrIndex = -1;
	private ReplicaValidationNotifier validationNotifier = null;

	// private boolean finished = false;
	// private boolean passed = false;

  public ValidationData(ConnectionData connectionData, String query,
      String value, String timeStamp, int serverIndex) {
    connData = connectionData;
    validationQuery = query;
    expectedValue = value;
    tStamp = timeStamp;
    svrIndex = serverIndex;
	boolean finished = false;
  }

  public ValidationData(ConnectionData connectionData, String query,
      String value, String timeStamp, int serverIndex, ReplicaValidationNotifier notifier) {
    connData = connectionData;
    validationQuery = query;
    expectedValue = value;
    tStamp = timeStamp;
    svrIndex = serverIndex;
	validationNotifier = notifier;
	boolean finished = false;
  }

  public void setServerIndex(int serverIndex) {
    svrIndex = serverIndex;
  }

  public void setTimeStamp(String timeStamp) {
    tStamp = timeStamp;
  }

  public void setValidationQuery(String query) {
    validationQuery = query;
  }

  public void setExpectedValue(String value) {
    expectedValue = value;
  }

  public void setConnectionData(ConnectionData connectionData) {
    connData = connectionData;
  }

  public String getValidationQuery() {
    return validationQuery;
  }

  public String getExpectedValue() {
    return expectedValue;
  }

  public String getTimeStamp() {
    return tStamp;
  }

  public ConnectionData getConnectionData() {
    return connData;
  }

  public int getServerIndex() {
    return svrIndex;
  }

	public ReplicaValidationNotifier getValidationNotifier() {
		return validationNotifier;
	}

}
