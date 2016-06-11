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

public abstract class RestStressTester extends StressTester {
    protected ConnectionData connData = null;
    protected RestSession session = null;

    public RestStressTester(ConnectionData connData, TestData testData, String threadName) {
        this.connData = connData;
        this.session = JavaAPISession.getRestSession(connData);
        this.testData = testData;
        this.threadName = threadName;
        isPerfTest = testData.getIsPerfTest();
        alive = true;
    }

    public RestStressTester() {
    }

    public void initializeTester(ConnectionData connData, TestData testData, String threadName) {
        this.connData = connData;
        this.session = JavaAPISession.getRestSession(connData);
        this.testData = testData;
        this.threadName = threadName;
        isPerfTest = testData.getIsPerfTest();
        alive = true;
    }

    protected RestSession getSession() {
        return session;
    }

    // METHOD runTest() a virtual method, sub classes must implement
    public abstract void runTest();

    public void connect() throws Exception {
        // nop;
    }

    public void disconnect() {
        // nop;
    }

    public void setTransactionTimeout(int t) throws Exception {
        // nop;
    }
}
