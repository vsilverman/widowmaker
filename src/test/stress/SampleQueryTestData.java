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
 * The SampleQueryTestData class is the data class for SampleQueryTester.
 *
 * @author Changfei
 * @version 1.0
 * @since 2015-10-27
 */

package test.stress;

public class SampleQueryTestData extends QueryTestData{
  
  SampleLoadTestData loadData = null;

  public SampleQueryTestData(String fileName) throws Exception {
    super(fileName);
    loadData = new SampleLoadTestData(fileName);
  }

}
