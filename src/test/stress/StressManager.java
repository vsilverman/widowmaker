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

import java.io.File;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.Handler;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Duration;

import test.telemetry.TelemetryServer;
import test.utilities.StressCmdProcessor;
import test.utilities.StressCmdHandler;

import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.SchedulerMetaData;
import org.quartz.SimpleTrigger;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.SchedulerException;

public class StressManager
  implements StressCmdHandler
{ // Xanax? //LOL

  String[] setupLocations = null;
  String[] testFiles = null;
  String[] scheduleFiles = null;
  String connectionFile = null;
  String resultsLogfile = null;
  private volatile boolean doShutdown = false;
  int minUsers = 0;
  int maxUsers = 0;
  int totalUsers = Integer.MAX_VALUE;
  int maxMinutes = Integer.MAX_VALUE;
  int telemetryPort = TelemetryServer.DEFAULT_TELEMETRY_PORT;
  int cmdPort = StressCmdProcessor.DEFAULT_PORT;
  int watcherPort = TelemetryWatcher.DEFAULT_PORT;
  volatile int currentTotalUsers = 0;
  int lastCurrentRunning = -1;
  int threadMgrOutputInterval = 0;
  // make sure we keep cycling through list of tests, not starting from
  // beginning of list every time
  int nextTestToStart = 0;
  ThreadManager threadMgr;
  ConnectionData connectionData;
  public static ReplicaValidationPool replicaValidationPool = null;
  public static ValidationManager validationMgr = null;
  public static StressCmdProcessor cmdProcessor = null;
  public static TelemetryServer telemetryServer = null;
  public static TelemetryWatcher telemetryWatcher = null;

  public static Scheduler scheduler = null;

  // public static ResultsLogger resultsLogger = null;
  static HashMap<String, ResultsLogger> loggers = null;
  DateFormat dateFormatter = null;
  long startupTime = 0L;

  private static StressManager theStressManager = null;

  private StressManager(String inputFileName)
    throws Exception {
    // parse the input file
    parseInput(inputFileName);

    initialize();
  }

  /**
   * constructor that allows the cmdline to override the connection info file
   * @inputFileName the name of the stress manager configuration file
   * @connectionFileName the name of the connection configuration file
   */
  private StressManager(String inputFileName, String connectionFileName)
    throws Exception {

    // parse the input file
    parseInput(inputFileName);

    // override the connection file
    connectionFile = connectionFileName;

    initialize();
  }

  /**
   * returns this singleton that is in place
   * @return the StressManager already created
   */
  public static StressManager getStressManager() {
    return theStressManager;
  }

  /**
   * No training wheels approach:  if there was one already there, this is going to overwrite it
   */
  public static StressManager getStressManager(String inputFileName)
            throws Exception {
    theStressManager = new StressManager(inputFileName);
    return theStressManager;
  }

  /**
   * No training wheels approach:  if there was one already there, this is going to overwrite it
   */
  public static StressManager getStressManager(String inputFileName, String connectionFileName)
          throws Exception {

    theStressManager = new StressManager(inputFileName, connectionFileName);
    return theStressManager;
  }

  protected void initialize()
    throws Exception {

    startupTime = System.currentTimeMillis();

    dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    telemetryServer = TelemetryServer.getTelemetryServer();

    // TODO: work out what port we're running telemetry on here
    //
    if (telemetryPort != 0)
      telemetryServer.setPort(telemetryPort);
    System.out.println("starting telemetry server on port " + telemetryPort);
    telemetryServer.initialize();

    threadMgr = new ThreadManager(maxUsers);
    connectionData = ConnectionDataManager.getConnectionData(connectionFile);
    if (replicasExist()) {
      replicaValidationPool = new ReplicaValidationPool(maxUsers);
      replicaValidationPool.start();
    }
    loggers = new HashMap<String, ResultsLogger>();

    // resultsLogger = new ResultsLogger();

    validationMgr = new ValidationManager();
    if (!validationMgr.isInitialized()) {
      validationMgr.start();
    }

    // TODO: turn this into a factory eventually
    cmdProcessor = StressCmdProcessor.getStressCmdProcessor();
    if (cmdPort != 0)
      cmdProcessor.setPort(cmdPort);

    System.out.println("starting cmd processor on port " + cmdPort);

    // TODO:  this needs to be looked at - is a local watcher really
    // only communicating through the port? It works, but is this what
    // we want to do?
    telemetryWatcher = new TelemetryWatcher("localhost", telemetryPort);
    if (watcherPort != 0)
      telemetryWatcher.setHttpPort(watcherPort);

    System.out.println("starting telemetry watcher on port " + watcherPort);

    threadMgr.initialize();
    cmdProcessor.initialize();
    telemetryWatcher.initialize();

    if (threadMgrOutputInterval > 0)
      threadMgr.setOutputInterval(threadMgrOutputInterval);

    cmdProcessor.addHandler(this);
    cmdProcessor.addHandler(telemetryServer);
    cmdProcessor.addHandler(cmdProcessor);

    SchedulerFactory sf = new StdSchedulerFactory();
    scheduler = sf.getScheduler();
    initializeScheduledJobs();
    scheduler.start();
  }

  protected void initializeScheduledJobs() {
  }

  protected void shutdownScheduledJobs(boolean force) {
    if (scheduler == null)
      return;

    try {
      scheduler.shutdown(force);
    } catch (SchedulerException e) {
      e.printStackTrace();
    }

  }

  /**
   * Shut down the entire testing. This method is available to a test
   * that was previously calling System.exit() - this seems a bit
   * more on the polite side.
   *
   * @force ignored for now, deciding if we need to have a way to
   * use draconian measures to clean this thing up.
   */
  public void requestShutdown(boolean force) {
    minUsers = 0;
    doShutdown = true;
    System.out.println("handling internal shutdown request");
    logMessage("handling internal shutdown request");
    threadMgr.stopAllTests();
    shutdownScheduledJobs(false);
  }

  public static ValidationManager getValidationManager() {
    return validationMgr;
  }

  public static StressCmdProcessor getCmdProcessor() {
    return cmdProcessor;
  }

  public static TelemetryServer getTelemetryServer() {
    return telemetryServer;
  }

  public static synchronized ResultsLogger getResultsLogger(String logFileName) {
    if (logFileName == null)
      return null;
    ResultsLogger logger = loggers.get(logFileName);
    if (logger == null) {
      logger = new ResultsLogger(logFileName);
      logger.init();
      loggers.put(logFileName, logger);
    }

    return logger;
  }

  public void logMessage(String msg) {
    if (loggers == null)
      return;
    Iterator iter = loggers.values().iterator();
    while (iter.hasNext()) {
      ResultsLogger logger = (ResultsLogger)iter.next();
      logger.logMessage(msg);
    }
  }

  public boolean replicasExist() {
    for (int i = 0; i < connectionData.servers.size(); i++) {
      if (connectionData.servers.get(i).replicas.size() > 0)
        return true;
    }
    return false;
  }

  public void handleCmd(String cmd, String line, PrintWriter out) {

    if (cmd.equals("status")) {
      out.print("StressManager:\r\n");
      DateTime startTime, nowTime;
      startTime = new DateTime(startupTime);
      nowTime = new DateTime(System.currentTimeMillis());
      Interval interval = new Interval(startTime, nowTime);
      Duration duration = interval.toDuration();
      out.print("start time:  " + new Date(startupTime).toString() + "\r\n");
      out.print("uptime:  " + duration + "\r\n");
      out.print("minUsers:  " + minUsers + "\r\n");
      out.print("maxUsers:  " + maxUsers + "\r\n");
      if (maxMinutes != Integer.MAX_VALUE)
        out.print("maxMinutes:  " + maxMinutes + "\r\n");
    }
    if (cmd.equals("uptime")) {
      DateTime startTime, nowTime;
      startTime = new DateTime(startupTime);
      nowTime = new DateTime(System.currentTimeMillis());
      Interval interval = new Interval(startTime, nowTime);
      Duration duration = interval.toDuration();
      out.print("start time:  " + startTime.toString() + "\r\n");
      out.print("uptime:  " + duration + "\r\n");
    }
    if (cmd.equals("help")) {
      out.print("StressManager:\r\n");
      out.print("shutdown\r\n");
      out.print("uptime\r\n");
      out.print("minUsers  <users>\r\n");
      out.print("maxUsers  <users> \r\n");
      out.print("add <test_file>\r\n");
      out.print("xcc_logging <new_level>\r\n");
      out.print("get_property <prop_name>\r\n");
      out.print("set_property <prop_name> <value>\r\n");
      out.print("get_properties\r\n");
    }
    else if (cmd.equals("shutdown")) {
      out.print("StressManager shutting down\r\n");
      minUsers = 0;
      System.out.println("handling shutdown request");
      logMessage("handling shutdown request");
      threadMgr.stopAllTests();
      doShutdown = true;
    }
    else if (cmd.equals("minusers")) {
      StringTokenizer tokens = new StringTokenizer(line);
      // get past the actual command
      String s = tokens.nextToken();
      s = tokens.nextToken();
      int i = Integer.parseInt(s);
      if (i > 0)
        minUsers = i;
      if (telemetryServer != null)
        telemetryServer.sendTelemetry("StressManager.minusers", Integer.toString(minUsers));
    }
    else if (cmd.equals("maxusers")) {
      StringTokenizer tokens = new StringTokenizer(line);
      // get past the actual command
      String s = tokens.nextToken();
      s = tokens.nextToken();
      int i = Integer.parseInt(s);
      if (i > 0)
        maxUsers = i;
      if (telemetryServer != null)
        telemetryServer.sendTelemetry("StressManager.maxusers", Integer.toString(maxUsers));
    }
    else if (cmd.equals("xcc_logging")) {
      StringTokenizer tokens = new StringTokenizer(line);
      // get past the actual command
      String s = tokens.nextToken();
      s = tokens.nextToken();

      Logger logger = Logger.getLogger("com.marklogic.xcc");

      System.out.println("xcc logger name:  " + logger.getName());
      System.out.println("xcc logger level:  " + logger.getLevel());
      System.out.println("xcc logger handles:");
      Handler[] handlers = logger.getHandlers();
      System.out.println("handle count:  " + handlers.length);
      int ii;

      for (ii = 0; ii < handlers.length; ii++) {
        Handler handler = handlers[ii];
        System.out.println("handler " + ii + ": " + handler.toString() + ", level " + handler.getLevel());
      }

      // initialize it to what it was
      Level newLevel = logger.getLevel();

      if (s.equalsIgnoreCase("OFF")) {
        newLevel = Level.OFF;
      } else if (s.equalsIgnoreCase("SEVERE")) {
        newLevel = Level.SEVERE;
      } else if (s.equalsIgnoreCase("WARNING")) {
        newLevel = Level.WARNING;
      } else if (s.equalsIgnoreCase("INFO")) {
        newLevel = Level.INFO;
      } else if (s.equalsIgnoreCase("CONFIG")) {
        newLevel = Level.CONFIG;
      } else if (s.equalsIgnoreCase("FINE")) {
        newLevel = Level.FINE;
      } else if (s.equalsIgnoreCase("FINER")) {
        newLevel = Level.FINER;
      } else if (s.equalsIgnoreCase("FINEST")) {
        newLevel = Level.FINEST;
      } else if (s.equalsIgnoreCase("ALL")) {
        newLevel = Level.ALL;
      }

      logger.setLevel(newLevel);

      // now walk through all the handlers and set them to the same level
      for (ii = 0; ii < handlers.length; ii++) {
        Handler handler = handlers[ii];
        handler.setLevel(newLevel);
        System.out.println("handler " + ii +
                            ": " + handler.toString() +
                            ", level " + handler.getLevel());
        // let the command client know we've handled this
        out.println("handler " + ii +
                            ": " + handler.toString() +
                            ", level " + handler.getLevel());
      }

      if (telemetryServer != null)
        telemetryServer.sendTelemetry("StressManager.xcc.logging_level", s);
    } else if (cmd.equals("test")) {
      StringTokenizer tokens = new StringTokenizer(line);
      String s = tokens.nextToken(); // trim off the initial command
      String subcmd = null;
      if (tokens.hasMoreTokens())
        subcmd = tokens.nextToken().toLowerCase();
      if (subcmd != null && subcmd.equals("start")) {
        String t = null;
        if (tokens.hasMoreTokens())
          t = tokens.nextToken();
        out.print("starting test " + t + "\r\n");
        if (t != null) {
          t = t.replaceAll("QA_HOME", System.getProperty("QA_HOME"));
          File f = new File(t);
          if (!f.exists()) {
            out.print("invalid filename:  " + t + "\r\n");
          } else {
            threadMgr.addCmdUser(connectionData, t, 0);
          }
        }
      }
      if (subcmd != null && subcmd.equals("add")) {
        String t = null;
        if (tokens.hasMoreTokens())
          t = tokens.nextToken();
        out.print("adding test " + t + "\r\n");
        if (t != null) {
          t = t.replaceAll("QA_HOME", System.getProperty("QA_HOME"));
          File f = new File(t);
          if (!f.exists()) {
            out.print("invalid filename:  " + t + "\r\n");
          } else {

            // threadMgr.addCmdUser(connectionData, t, 0);
            addTestLocation(t);
          }
        }
      }
    } else if (cmd.equals("set_property")) {
      StringTokenizer tokens = new StringTokenizer(line);
      // get past the actual command
      String s = tokens.nextToken();
      String prop = tokens.nextToken();
      String value = null;
      if (tokens.hasMoreTokens())
        value = tokens.nextToken();
      StressTestProperties properties = StressTestProperties.getStressTestProperties();
      if (value != null)
        properties.setProperty(prop, value);
      else
        out.print("no value provided\r\n");
    } else if (cmd.equals("get_property")) {
      StringTokenizer tokens = new StringTokenizer(line);
      // get past the actual command
      String s = tokens.nextToken();
      String prop = tokens.nextToken();
      StressTestProperties properties = StressTestProperties.getStressTestProperties();
      String value = properties.getProperty(prop);
      if (value == null)
        out.print("not set\r\n");
      else
        out.print(value + "\r\n");
    } else if (cmd.equals("get_properties")) {
      StringTokenizer tokens = new StringTokenizer(line);
      // get past the actual command
      String s = tokens.nextToken();
      StressTestProperties properties = StressTestProperties.getStressTestProperties();
      Enumeration propNames = properties.getPropertyNames();
      while (propNames.hasMoreElements()) {
        String prop = (String)propNames.nextElement();
        String value = properties.getProperty(prop);
        out.print(prop + ": " + value + "\r\n");
      }
    }
  }

  // Starts Max # of tests to start
  public void startTests() {
    startScheduleSet(0);
    startNextUserSet(maxUsers);
  }

  // Starts new user(s) if the number of users falls below
  // the minimum running
  void startNextUserSet(int numToStart) {
    // if number of users is below min increase to max
    for (int i = 0; i < numToStart; i++) {
      if (nextTestToStart == testFiles.length) {
        nextTestToStart = 0;
      }
      /*
       * try{ Thread.sleep(1000); }catch(Exception e) {}
       */
      System.out.println("Starting user " + i + " with nextTestToStart " + nextTestToStart);
      // this was passing in i, it's a counter, not a position
      if (testFiles.length == 0) {
        System.out.println("testFiles is empty");
      } else {
        startUser(testFiles[nextTestToStart++], i);
      }
    }

    if (telemetryServer != null) {
      telemetryServer.sendTelemetry("StressManager.nextTestToStart",
            Integer.toString(nextTestToStart));
      telemetryServer.sendTelemetry("StressManager.minusers",
            Integer.toString(minUsers));
      telemetryServer.sendTelemetry("StressManager.maxusers",
            Integer.toString(maxUsers));
    }
  }

  // no notion of type just test case
  void startUser(String inputFilePath, int pos) {
    // inputFilePath = tests.getUser(userType);
    // gets inputFilePath of specified type
    if (inputFilePath != null && inputFilePath.length() != 0) {
      if (currentTotalUsers < totalUsers) {
        if (threadMgr.addUser(connectionData, inputFilePath, pos)
            && totalUsers != Integer.MAX_VALUE)
          currentTotalUsers++;
      }
    } else
      System.err.println("Please check your set up and try again");
  }

  // Starts new user(s) if the number of users falls below
  // the minimum running
  void startScheduleSet(int numToStart) {
    // if number of users is below min increase to max
    if ((scheduleFiles == null) || (scheduleFiles.length == 0))
      return;

    for (int i = 0; i < scheduleFiles.length; i++) {
      /*
       * try{ Thread.sleep(1000); }catch(Exception e) {}
       */
      System.out.println("Starting schedule " + i + " with nextTestToStart " + nextTestToStart);
      // this was passing in i, it's a counter, not a position
      if (scheduleFiles.length == 0) {
        System.out.println("scheduleFiles is empty");
      } else {
        startSchedule(scheduleFiles[i], i);
      }
    }

/*
    if (telemetryServer != null) {
      telemetryServer.sendTelemetry("StressManager.nextTestToStart",
            Integer.toString(nextTestToStart));
      telemetryServer.sendTelemetry("StressManager.minusers",
            Integer.toString(minUsers));
      telemetryServer.sendTelemetry("StressManager.maxusers",
            Integer.toString(maxUsers));
    }
*/
  }

  void startOneSchedule(String inputFilePath, int pos) {

    System.out.println("StressManager.startOneSchedule");

    try {
      String testType = new TestData(inputFilePath).getTestType();
      System.out.println("test type is " + testType);
      StressTester st = StressTest.getStressTester(testType);
      if (st != null) {
        if (!(st instanceof SchedulableStressTester)) {
          System.out.println("testType is not loadable:  " + testType);
        } else {
          LoadableStressTester lt = (LoadableStressTester)st;
          SchedulableStressTester schedulableTester = (SchedulableStressTester)st;

          System.out.println("initializing stress test");

          TestData data = lt.getTestDataInstance(inputFilePath);
          st.initialize(data, Integer.toString(pos));
        
          // we're not going to run this - just get the trigger from it
          JobDetail detail = schedulableTester.makeJobDetail(inputFilePath);
          Trigger trigger = schedulableTester.makeTrigger(data);

          System.out.println("adding trigger to scheduler");

          scheduler.scheduleJob(detail, trigger);
        }
      } else {
        System.out.println("StressManager:  No suitable tester found for testType " + testType);
      }
    }
    catch (SchedulerException e) {
      System.out.println("unable to schedule job:");
      e.printStackTrace();
    }
    catch (Exception e) {
      System.out.println("unable to create schedule:  "
                          + inputFilePath);
      e.printStackTrace();
    }
  }

  // no notion of type just test case
  void startSchedule(String inputFilePath, int pos) {
    // inputFilePath = tests.getUser(userType);
    // gets inputFilePath of specified type
    try {
    if (inputFilePath != null && inputFilePath.length() != 0) {
      startOneSchedule(inputFilePath, pos);
/*
      String testType = new TestData(inputFilePath).getTestType();

      StressTest test = new StressTest(testType);

      if (currentTotalUsers < totalUsers) {
        if (threadMgr.addUser(connectionData, inputFilePath, pos)
            && totalUsers != Integer.MAX_VALUE)
          currentTotalUsers++;
      }
*/
    } else
      System.err.println("Please check your set up and try again");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  int runTests() {

    long initTimeStamp = System.currentTimeMillis();

    startTests();
    System.out.println("Started The Tests");
    int currentRunning = 0;

    int retVal = 0;

    try {
      while ((!doShutdown) && (currentTotalUsers < totalUsers)) {
        try {
          // sleep and let the threads work
          Thread.sleep(15000);

          // see if the tests should stop
          /*
           * if(cmd.getStop()) { System.out.println("Terminating threads" );
           * threadMgr.stopAllTests(); System.out.println(
           * "Threads have recieved the stop signal" ); alive = false; retVal =
           * 1; //recieved stop cmd; }
           */

          // make sure there are enough users running
          currentRunning = threadMgr.getCurrRunning();
          if (currentRunning != lastCurrentRunning) {
            lastCurrentRunning = currentRunning;
            if (telemetryServer != null)
              telemetryServer.sendTelemetry("StressManager.current_running",
                  Integer.toString(currentRunning));
          }
          endFinishedTests();
          if ((!doShutdown) && ((maxUsers - currentRunning) > 0)
            && (currentRunning <= minUsers)) {
              startNextUserSet(maxUsers - currentRunning);
          } else {
            System.out.println(dateFormatter.format(new Date()));
            System.out.println("Currently running " + currentRunning);
            System.out.println("MinUsers " + minUsers);
          }

          long timeStamp = System.currentTimeMillis();
          int minutes = (int) ((timeStamp - initTimeStamp) / (60 * 1000));
          if (minutes >= maxMinutes) {
            System.out.println("shutting down:  maxMinutes reached " + maxMinutes);
            threadMgr.stopAllTests();
            doShutdown = true;
            // return retVal;
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    // shutdown process
    try {
      while(currentRunning > 0) {

        // shut down the job scheduler
        shutdownScheduledJobs(false);

        Thread.sleep(15000);
        currentRunning = threadMgr.getCurrRunning();
        if (currentRunning != lastCurrentRunning) {
          lastCurrentRunning = currentRunning;
          if (telemetryServer != null)
            telemetryServer.sendTelemetry("StressManager.current_running",
                Integer.toString(currentRunning));
        }
        endFinishedTests();
        System.out.println(dateFormatter.format(new Date()));
        System.out.println("handling shutdown request");
        System.out.println("Currently running " + currentRunning);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    shutdownScheduledJobs(true);

    System.out.println("StressManager has completed");

    return retVal;
  }

  int endFinishedTests() {
    // System.out.println("Entering int  endFinishedTests()");
    int retVal = 0;
    int userPosition = 0;
    while (threadMgr.getDeadPosition() != -1) {
      userPosition = threadMgr.getDeadPosition();
      if (userPosition != -1) {
        System.out.println("Deleting user " + userPosition);
        threadMgr.deleteUser(userPosition);
        retVal++;
      }
    }
    // System.out.println("Exiting int  endFinishedTests() " + userPosition);
    // if something got cleaned up, let's force the output to display
    if (retVal > 0)
	    threadMgr.getThreadStates(true);
    else
      threadMgr.getThreadStates(false);
    return retVal; // meaningless now but will return usefull info with
                   // reporting

  }

  void parseInput(String fileName) throws Exception {
    // this should parse a xml file that contains the path
    // to connectionInfo as well as xml input files
    // should have min users and max users values
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    File inputFile = new File(fileName);
    if (!inputFile.canRead()) {
      throw new IOException("missing or unreadable inputPath: "
          + inputFile.getCanonicalPath());
    }

    Document stressManagerDocument = builder.parse(inputFile);
    NodeList nodeList = stressManagerDocument.getDocumentElement()
        .getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      if (nodeList.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      String tagName = ((Element) nodeList.item(i)).getTagName();
      if (tagName.equalsIgnoreCase("connect-location")) {
        setConnectLocation((Element) nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("min-users")) {
        setMinMax((Element) nodeList.item(i), true);
      } else if (tagName.equalsIgnoreCase("max-users")) {
        setMinMax((Element) nodeList.item(i), false);
      } else if (tagName.equalsIgnoreCase("total-users")) {
        setTotalUsers((Element) nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("max-minutes")) {
        setMaxMinutes((Element) nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("setup-locations")) {
        setSetupLocations((Element) nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("stress-tests")) {
        setTestLocations((Element) nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("schedule-tests")) {
        setScheduleTestLocations((Element) nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("results-logfile")) {
        setResultsLogfile((Element) nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("telemetry-port")) {
        setTelemetryPort((Element) nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("cmd-port")) {
        setCmdPort((Element) nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("watcher-port")) {
        setWatcherPort((Element) nodeList.item(i));
      } else if (tagName.equalsIgnoreCase("output-interval")) {
        int value = Integer.parseInt(getNodeText(nodeList.item(i)));
        setOutputInterval(value);
      }
    }
  }

  public synchronized void setSetupLocations(Element setUpLocations) {
    NodeList nodeList = ((Element) setUpLocations)
        .getElementsByTagName("setup-location");
    setupLocations = new String[nodeList.getLength()];
    for (int i = 0; i < nodeList.getLength(); i++) {
      if (nodeList.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      setupLocations[i] = getNodeText((Element) nodeList.item(i));
    }
  }

  public synchronized void setTestLocations(Element stressTests) {
    NodeList nodeList = ((Element) stressTests)
        .getElementsByTagName("test-location");
    testFiles = new String[nodeList.getLength()];
    for (int i = 0; i < nodeList.getLength(); i++) {
      if (nodeList.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      testFiles[i] = getNodeText((Element) nodeList.item(i)).replaceAll(
          "QA_HOME", System.getProperty("QA_HOME"));
      ;
    }
  }

  public synchronized void setScheduleTestLocations(Element stressTests) {
    NodeList nodeList = ((Element) stressTests)
        .getElementsByTagName("test-location");
    scheduleFiles = new String[nodeList.getLength()];
    for (int i = 0; i < nodeList.getLength(); i++) {
      if (nodeList.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      scheduleFiles[i] = getNodeText((Element) nodeList.item(i)).replaceAll(
          "QA_HOME", System.getProperty("QA_HOME"));
      System.out.println("set up scheduled test:  " + scheduleFiles[i]);
    }
  }

  public synchronized void addTestLocation(String testToAdd) {

    if (testToAdd == null)
      return;

    for (int ii = 0; ii < testFiles.length; ii++) {
      if (testFiles[ii] == null) {
        testFiles[ii] = testToAdd;
        return;
      }
    }
    // we fall out the bottom - there must be no room at the inn
    int newSize = testFiles.length + 1;
    System.out.println("resizing testFiles to " + newSize);
    String[] newTests = new String[newSize];
    for (int ii = 0; ii < testFiles.length; ii++)
      newTests[ii] = testFiles[ii];
    newTests[newSize-1] = testToAdd;
    testFiles = newTests;
  }

  public void setMinMax(Element minMax, boolean isMin) {
    try {
      int value = Integer.parseInt(getNodeText(minMax));
      if (isMin) {
        minUsers = value;
      } else {
        maxUsers = value;
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void setTotalUsers(Element total) {
    try {
      int value = Integer.parseInt(getNodeText(total));
      totalUsers = value;

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void setMaxMinutes(Element total) {
    try {
      int value = Integer.parseInt(getNodeText(total));
      maxMinutes = value;

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * this is a value passed to the Thread Manager, but it may not be
   * instantiated as yet
   */
  public void setOutputInterval(int value) {
    threadMgrOutputInterval = value;
    if (threadMgr != null)
      threadMgr.setOutputInterval(threadMgrOutputInterval);
  }

  public void setConnectLocation(Element connectLocation) {
    connectionFile = getNodeText(connectLocation);
    connectionFile = connectionFile.replaceAll("QA_HOME",
        System.getProperty("QA_HOME"));
  }

  public void
  setResultsLogfile(Element logfileLocation) {
    resultsLogfile = getNodeText(logfileLocation);
    resultsLogfile = resultsLogfile.replaceAll("QA_HOME",
        System.getProperty("QA_HOME"));
    resultsLogfile = resultsLogfile.replaceAll("TMP_DIR",
        System.getProperty("TMP_DIR"));
  }

  /**
   * TODO:  it would be nice if we could pass a config element in to the
   * telemetry server and let it pluck this stuff out itself
   */
  public void setTelemetryPort(Element data) {
    try {
      int value = Integer.parseInt(getNodeText(data));
      telemetryPort = value;

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * TODO:  it would be nice if we could pass a config element in to the
   * telemetry server and let it pluck this stuff out itself
   */
  public void setCmdPort(Element data) {
    try {
      int value = Integer.parseInt(getNodeText(data));
      cmdPort = value;

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * TODO:  it would be nice if we could pass a config element in to the
   * telemetry server and let it pluck this stuff out itself
   */
  public void setWatcherPort(Element data) {
    try {
      int value = Integer.parseInt(getNodeText(data));
      watcherPort = value;

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public String
  getResultsLogfile() {
    return resultsLogfile;
  }

  private String getNodeText(Node t) {
    if ((t == null) || (t.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE))
      return null;
    NodeList children = t.getChildNodes();
    String text = "";
    for (int c = 0; c < children.getLength(); c++) {
      Node child = children.item(c);
      if ((child.getNodeType() == org.w3c.dom.Node.TEXT_NODE)
          || (child.getNodeType() == org.w3c.dom.Node.CDATA_SECTION_NODE))
        text += child.getNodeValue();
    }
    return text;
  }
}
