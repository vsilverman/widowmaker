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

/****************************************************************************
 * class StressTest
 * It's the driver Stress testcases.
 * 
 *****************************************************************************/

package test.stress;

import java.io.InputStream;
import java.io.IOException;
import java.util.*;

import test.sql.TemplateDataLoadTester;
import test.sql.TemplateLoadTestData;
import test.sql.TemplateDataUpdateTester;
import test.sql.TemplateDataModifyTester;

public class StressTest extends Thread {
  // private Runtime runTime = null;
  // public ThreadGroup stressGroup = null;
  private ConnectionData connectionData;
  private String inputFile;
  private String testType;
  private int pos;
  private StressTester tester = null;

  private static HashMap<String, Class> classHash = new HashMap<String, Class>();
  private static final boolean classLoaderDebugFlag = false;


    public StressTest(ConnectionData connectionData, String inputFile, int pos) {
        this.connectionData = connectionData;
        this.inputFile = inputFile;
        this.pos = pos;
    }

    public void setStop() {
    if (tester != null) {
      tester.setAlive(false);
    }
  }

  static void addClassToHash(String testType, String className) {
    if (testType == null || className == null)
      return;

    synchronized (classHash) {
      Class c = null;
      c = (Class)classHash.get(testType);
      if (c != null) {
        if (classLoaderDebugFlag)
          System.out.println("duplicate class entry for " + testType);
        return;
      }
      try {
          c = Class.forName(className);
          classHash.put(testType, c);
      } catch (ClassNotFoundException e) {
        System.out.println("Class not found:  " + className);
      }
    }
  }

  static StressTester getStressTester(String testType) {
    if (testType == null)
      return null;

    StressTester st = null;

    try {
      if (classLoaderDebugFlag)
        System.out.println("loading class for testType " + testType);
      Class c = null;
      synchronized(classHash) {
        c = classHash.get(testType);
        if (c == null) {
          if (classLoaderDebugFlag)
            System.out.println("not found in classHash:  testType " + testType);
        } else {
          if (classLoaderDebugFlag)
            System.out.println("found:  testType, className = " + testType + ", " + c.getName());
          Object o = c.newInstance();
          st = (StressTester)o;
        }
      }
      
    }
    catch (InstantiationException e) {
      e.printStackTrace();
    }
    catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return st;
  }

  public void setAlive(boolean aliveVal) {
    if (tester != null) {
      tester.setAlive(aliveVal);
    }
  }

  public boolean getAlive() {
    return tester != null && tester.isAlive();
  }

  public String toString() {
	return testType + " (" + inputFile + ")";
  }

  public String getInputFile() {
    return inputFile;
  }

  private Class getClassForTestType(String testType) {

    if (testType == null)
      return null;

    if (classLoaderDebugFlag)
      System.out.println("getClassForTestType:  " + testType);

    Class c = null;

    synchronized(classHash) {
      c = classHash.get(testType);
      if (c == null) {
        if (classLoaderDebugFlag)
          System.out.println("no class found for testType " + testType);
      } else {
        String className = c.getName();
        if (classLoaderDebugFlag)
          System.out.println("testType, className:  " + testType + ", " + className);
      }
    }

    return c;
  }

  public static void loadStressTestExtensions() {

    System.out.println("loadStressTestExtensions");
    StressTestProperties props = StressTestProperties.getStressTestProperties();
    int i = 1;
    while (true) {
      String propName = "stress.extension." + i;
      String className = props.getProperty(propName);
      if (className == null) {
        System.out.println("finished locating extensions at " + i);
        break;
      }
      System.out.println("Processing property:  " + propName);
      System.out.println("Resulting class name:  " + className);

      Class c = null;
      Object o = null;
      if (className != null) {
        try {
          c = Class.forName(className);
        } catch (ClassNotFoundException e) {
          System.out.println("No stress test class found for " + className);
          return;
        }
        if (c == null) {
          System.out.println("No stress test class found for " + className);
          return;
        }
        
        try {
          o = c.newInstance();
        } catch (InstantiationException e) {
          System.out.println("Class can not be instantiated:  " + className);
          e.printStackTrace();
          return;
        } catch (IllegalAccessException e) {
          System.out.println("Class can not be instantiated:  " + className);
          e.printStackTrace();
          return;
        }
        if (!(o instanceof LoadableStressTester)) {
          System.out.println("Class is not of type StressTester:  " + className);
          return;
        }
        String testType = ((LoadableStressTester)o).getTestType();
        System.out.println("Test type:  " + testType);
        addClassToHash(testType, className);
      } else {
        break;
      }
      ++i;
    }

  }

  private static final String DEFAULT_PROPERTIES_FILE = "StressTestDefaults.xml";

  private static Properties getDefaultProperties() {

    Properties defaultProps = null;
    InputStream is = null;

    try {
      is = StressTest.class.getResourceAsStream(DEFAULT_PROPERTIES_FILE);
      if (is != null) {
        defaultProps = new Properties();
        defaultProps.loadFromXML(is);
      }
    } catch (IOException e) {
      System.out.println("error processing default properties file");
    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (IOException e2) {
          // do nothing
        }
      }
    }

    return defaultProps;
  }

  public void run() {
    System.out.println("*****************entering run" + pos);
    tester = null;
    try {
      testType = (new TestData(inputFile)).getTestType();
      for (int i = 0; i < numberOfLoops; i++) {
        if (testType.equalsIgnoreCase("loadTester")) {
          tester = new XccLoadTester(connectionData, new LoadTestData(inputFile),
              Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("restLoadTester")) {
            tester = new RestLoadTester(connectionData, new LoadTestData(inputFile),
                    Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("bulkLoadTester")) {
            tester = new BulkLoadTester(connectionData, new LoadTestData(inputFile),
                    Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("bulkQueryTester")) {
            tester = new BulkQueryTester(connectionData, new QueryTestData(inputFile),
                    Integer.toString(pos), Integer.toString(i));
        } else if (testType.equalsIgnoreCase("pojoLoadTester")) {
            tester = new PojoLoadTester(connectionData, new LoadTestData(inputFile),
                    Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("evalLoadTester")) {
            tester = new EvalLoadTester(connectionData, new LoadTestData(inputFile),
                    Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("ExtensionLoadTester")) {
            tester = new ExtensionLoadTester(connectionData, new LoadTestData(inputFile),
                    Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("crudTester")) {
            tester = new CRUDTester(connectionData, new CRUDTestData(inputFile),
                    Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("restCrudTester")) {
            tester = new RestCRUDTester(connectionData, new CRUDTestData(inputFile),
                    Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("hashLockTester")) {
          tester = new HashLockTester(connectionData, new HashLockTestData(
              inputFile), Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("queryTester")) {
            tester = new QueryTester(connectionData, new QueryTestData(
                    inputFile), Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("semQueryTester")) {
            tester = new SemQueryTester(connectionData, new QueryTestData(
                    inputFile), Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("restQueryTester")) {
            tester = new RestQueryTester(connectionData, new QueryTestData(
                    inputFile), Integer.toString(pos), Integer.toString(i));
        } else if (testType.equalsIgnoreCase("sqlTester")) {
            tester = new SQLTester(connectionData, new QueryTestData(
                inputFile), Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("inferenceTester")) {
            System.out.println("about to create InferenceQueryTester");
            tester = new InferenceQueryTester(connectionData, new InferenceTestData(
                inputFile), Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("inferenceLoadTester")) {
            System.out.println("about to create InferenceLoadTester");
            tester = new InferenceLoadTester(connectionData, new InferenceLoadTestData(
                inputFile), Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("dbTradeStore")) {
          tester = new DbTradeStoreLoadTester(connectionData,
                            new DbTradeStoreTestData(inputFile),
              Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("sparqlUpdateTester")) {
          tester = new SPARQLUpdateTester(connectionData, new QueryTestData(
                          inputFile), Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("semanticGraphs")) {
          tester = new SemanticGraphLoadTester(connectionData,
                    new LoadTestData(inputFile), Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("HttpStressTester")) {
          tester = new HttpStressTester(connectionData, new HttpTestData(
                          inputFile), Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("ParserTester")) {
          tester = new ParserTester(connectionData,
                            new ParserTestData(inputFile),
                            Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("XccInvokeTester")) {
          tester = new XccInvokeTester(connectionData,
                            new InvokeTestData(inputFile),
                            Integer.toString(pos));
        } else if (testType.equalsIgnoreCase("SampleLoadTester")) {
/*
 * this did not get pulled over from 8.0 tree
        	tester = new SampleLoadTester(connectionData,
        					new SampleLoadTestData(inputFile),
        					Integer.toString(pos));
*/
        } else if (testType.equalsIgnoreCase("SampleCRUDTester")) {
/*
        	tester = new SampleCRUDTester(connectionData,
        					new SampleCRUDTestData(inputFile),
        					Integer.toString(pos));
*/
        } else if (testType.equalsIgnoreCase("SampleQueryTester")) {
/*
            tester = new SampleQueryTester(connectionData,
                            new SampleQueryTestData(inputFile),
                            Integer.toString(pos));
*/
        } else {
          // let's try to classload this thing
          StressTester st = getStressTester(testType);
          if (st != null) {
            if (!(st instanceof LoadableStressTester)) {
              System.out.println("testType is not loadable:  " + testType);
            } else {
              tester = (StressTester)st;
              LoadableStressTester loadableTester = (LoadableStressTester)st;
              TestData data = loadableTester.getTestDataInstance(inputFile);

              /*
               * Legacy extensions that are derivatives of XccStressTester will expect
               * to be constructed with connection data passed in. Extensions such as
               * REST extensions don't need an XCC connection and have no need to be handed
               * one. New test extensions - and those written outside the company - should
               * follow a new model where the connection information is retrieved as needed
               * from the ConnectionDataManager.
               */
                tester.initialize(data, Integer.toString(pos));
            }
          } else {
            System.out.println("No suitable tester found for testType " + testType);
          }
        }

        if (tester != null)
          tester.runTest();
        tester = null;
        System.out.println("Thread #" + Integer.toString(pos)
            + ": finished loop #" + (i + 1) + " ***********************\n\n");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("*************exiting run" + pos);
  }

  // ****************************************************************************************************

  private static List<String> inputFiles = new ArrayList<String>();
  private static String connectionFile = null;
  private static String stressManagerFile = "";
  private static String stressTestPropertiesFile = null;
  private static int numberOfLoops = 1; // to repeat this test by looping

  public static void runStress() throws Exception {
    StressTest[] tests = new StressTest[inputFiles.size()];
    System.out.println(inputFiles.size());
    // stressTest.runTime = Runtime.getRuntime();
    // stressTest.stressGroup = new ThreadGroup("stressThreadGroup");

    ConnectionData connectionData = ConnectionDataManager.getConnectionData(connectionFile);

    for (int i = 0; i < inputFiles.size(); i++) {
      tests[i] = new StressTest(connectionData, inputFiles.get(i), i);
      System.out.println("Thread #" + (i) + ": starting ...... ");
      tests[i].start();
    }
    boolean finished = false;
    while (!finished) {
      try {
        Thread.sleep(10000);

        // stressCmd.run();
        for (int i = 0; i < inputFiles.size(); i++) {
          System.out.println("in for loop" + i);
          if (tests[i].getAlive()) {
            System.out.println("found at least one alive");
            System.out.println("number of input files " + inputFiles.size());
            System.out.println("found one not done");
            break;
            // i = inputFiles.size() + 1; //at least one active
          } else {
            System.out.println("at last one and it is dead");
            if (i + 1 == inputFiles.size())
              finished = true;
            System.out.println("after setting finished to true");
          }
        }

        Thread.sleep(10000);
        System.out.println("after sleep finished val " + finished);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    // System.out.println("leaving run stress");
  }

  // main method: start each test in a separate thread
 public static void main(String[] args) {
      /* the settings below have been replaced by stress/widowmaker/log4j.properties
       * since log4j will allow us to get the thread name with each log message so we can
       * separate out the wire traces by thread
      java.util.logging.Logger.getLogger("org.apache.http.wire").setLevel(java.util.logging.Level.FINEST);
      java.util.logging.Logger.getLogger("org.apache.http.headers").setLevel(java.util.logging.Level.FINEST);

      System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
      System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
      System.setProperty("org.apache.commons.logging.simplelog.log.httpclient.wire", "warn");
      System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http", "debug");
      System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.headers", "warn");

      System.setProperty("javax.net.debug", "all");
      */

      parseOptions(args);
    try {
      Properties defaultProps = getDefaultProperties();
      StressTestProperties props = StressTestProperties.getStressTestProperties();
      if (stressTestPropertiesFile != null) {
        props.initialize(stressTestPropertiesFile, defaultProps);
      } else {
        props.initialize(defaultProps);
      }

      loadStressTestExtensions();

      if (stressManagerFile != "") {
        if (connectionFile != null) {
          (StressManager.getStressManager(stressManagerFile, connectionFile)).runTests();
        } else {
          (StressManager.getStressManager(stressManagerFile)).runTests();
        }
      }
      else
        runStress();
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.exit(0);
  }  
  
  /*public static void main(String[] args) { 
    System.setProperty("QA_HOME", "/space/mlsrcs/qa");
    try {
      new StressManager("/space/mlsrcs/qa/stress/widowmaker/MathTests/MathStressTests.xml").runTests();
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.exit(0);
  }*/

  private static boolean parseOptions(String[] options) {
    int i, l = options.length;
    if (l < 1) {
      usage();
      return false;
    }
    for (i = 0; i < l; i++) {
      System.out.println(options[i]);
      if (!options[i].startsWith("-")) {
        System.out.println("options did not start with -" + options[i] + "\n");
        return false;
      }
      if (options[i].equals("-t")) { // following are inputfile names
        while (++i < l && !options[i].startsWith("-")) {
          inputFiles.add(options[i]);
        }
      }
      if (options[i].equals("-c")) { // connection file
        connectionFile = options[++i];
        System.out.println("setting connection file to " + connectionFile);
      }
      if (options[i].equals("-s")) { // testManager file
        stressManagerFile = options[++i];
        System.out.println("setting stress manager file to " + stressManagerFile);
      }
      if (options[i].equals("-props")) { // testManager file
        stressTestPropertiesFile = options[++i];
        System.out.println("setting stress test properties file to " + stressTestPropertiesFile);
      }
      if (i < l && options[i].equals("-l")) { // number of loops
        if ((i + 1) >= l || options[i + 1].startsWith("-"))
          return false;
        try {
          numberOfLoops = (new Integer(options[++i])).intValue();
        } catch (NumberFormatException exc) {
          System.out.println("Error: NumberOfLoops is not an integer. \n");
          return false;
        }
      }

      if (i < l && options[i].equals("-h")) { // help option, just return
        System.out.println("i < l or asked for help\n");
        return false;
      }
    }

    if (stressManagerFile.equals("") && inputFiles.isEmpty()) {
      System.out.println("Error: Input File is not specified. \n");
      return false;
    }

    return true; // command line is parsed correctly
  }

  // METHOD usage() print to the screen the command line options
  private static void usage() {
    System.out.println("Usage: java test.stress.StressTest (options)");
    System.out.println("options:");
    System.out
        .println("  -t testcaseInputFileName ... (REQUIRED if running individual test case)");
    System.out
        .println("  -c connectionInfo.xml ... (REQUIRED if running individual test case)");
    System.out
        .println("  -s stressManager.xml (REQIRED if having stress manager control tests)");
    System.out
        .println("  -props properites.xml (Properties file to configure StressTest)");
    System.out.println("  -l numberOfLoops   (Default to 1 if not specified)");
    System.out.println("  -h print out this help screen.");
  }
}
