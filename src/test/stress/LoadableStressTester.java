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

/**
 * Signature interface for all classes which are loadable as a dynamic
 * stress test extension. All loadable extensions need to implement
 * this interface in addition to extending StressTester or one of its
 * derivatives. Loading of the test extension table checks that the
 * class specified is an instanceof this interface.
 */
public interface LoadableStressTester {

    /**
     * @return the string that is the token found in stress test config files
     * as <testtype> in order to know which class is the implementation
     */
    public String getTestType();

    /**
     * Loadable stress testers know what type of TestData derivative they act
     * upon, and are expected to produce the object themselves on request
     */
    public TestData getTestDataInstance(String filename)
        throws Exception;

    /**
     * Used instead of passing in these elements at construction time,
     * since construction is performed via newInstance(). This argument
     * set allows for both XCC-based and non-XCC-based extensions
     * being classloaded. Any XCC-based tester can fetch the needed
     * connection data via the ConnectionDataManager singleton
     */
    public void initialize(TestData data, String threadName)
        throws Exception;
}

