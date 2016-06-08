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

/**
 * The SampleCRUDTester class implements a stress tester for
 * deleting and updating documents, which are loaded by SampleLoadTester.
 * The loading process is done by SampleLoadTester.
 * The update and deletion are inheritated from CRUDTester.
 *
 * @author Changfei Chen
 * @version 1.0
 * @since 2015-10-27
 */

package test.stress;

public class SampleCRUDTester extends CRUDTester {
	
  SampleLoadTester loadTester = null;
  SampleLoadTestData loadData = null;

  public SampleCRUDTester(ConnectionData connData, SampleCRUDTestData crudData,
                          String threadName) {
    super(connData, crudData, threadName);	
    loadTester = new SampleLoadTester(connData, crudData.loadData, 
                                      threadName, this);
  }

  protected void loadContentFromDir(boolean rollback) 
                 throws InterruptedException {
    loadTester.loadContentFromDir(rollback);
  }

}
