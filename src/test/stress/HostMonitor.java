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

import java.util.HashMap;
import java.util.Map;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import test.utilities.TopMonitor;

import test.telemetry.TelemetryObserver;
import test.telemetry.TelemetryReader;
import test.telemetry.TelemetryServer;

public class HostMonitor
      extends Thread
      implements TelemetryObserver {

  public static final int DEFAULT_TELEMETRY_PORT = 33449;
  public static final long DEFAULT_SLEEP_INTERVAL = 5000L;
  public static final long DEFAULT_STATUS_DUMP_INTERVAL = 300000L;
  public static final String DEFAULT_RESULTS_LOG_FILE = "HostMonitor.log";

  boolean bDebug = false;
  boolean isInitialized = false;
  boolean isClientInitialized = false;
  protected volatile boolean shutdownRequested = false;
  protected int telemetryPort = 0;
  protected TelemetryServer telemetryServer = null;
  protected long sleepInterval = DEFAULT_SLEEP_INTERVAL;
  protected long statusInterval = DEFAULT_STATUS_DUMP_INTERVAL;
  protected long lastStatusDump = 0L;
  protected TelemetryReader telemetryReader = null;
  protected HashMap<String, String> telemetryData = null;
  // TODO:  still needs to be fixed
  protected String hostName = "localhost";
  protected int storedPid = -1;

  protected ResultsLogger logger = null;

  DateFormat dateFormatter = new SimpleDateFormat("YYYY-mm-dd HH:mm:ss");
  DateFormat lastUsedFormatter = new SimpleDateFormat("YYYY-mm-dd'T'HH:mm:ss.SSS");

  // temporary to figure out how we want to hook this all up
  protected long monitorInterval = 30000L;
  protected long lastMonitorRun = 0L;



  public HostMonitor() {
    telemetryPort = DEFAULT_TELEMETRY_PORT;
    lastStatusDump = System.currentTimeMillis();
    telemetryData = new HashMap<String, String>();
  }

  public HostMonitor(int telemetryPort) {
    this();
    this.telemetryPort = telemetryPort;
  }

  public void initialize()
      throws Exception {

    if (isInitialized)
      return;

    telemetryServer = new TelemetryServer(telemetryPort);
    telemetryServer.initialize();
    
    initPrivateTelemetryStrings();

    logger = new ResultsLogger(DEFAULT_RESULTS_LOG_FILE);
    logger.init();

    isInitialized = true;
  }

  /**
   * this is used strictly for debugging purposes and demo
   */
  protected void initClient()
      throws Exception {

    if (isClientInitialized)
      return;

    telemetryReader = new TelemetryReader("localhost", telemetryPort);
    telemetryReader.initialize();
    telemetryReader.start();

    telemetryReader.addTelemetryObserver(this);

    isClientInitialized = true;
  }

  public void shutdown() {

    shutdownRequested = true;

    if (telemetryReader != null) {
      telemetryReader.shutdown();
      telemetryReader = null;
    }

    if (telemetryServer != null) {
      telemetryServer.shutdown();
      telemetryServer = null;
    }

  }

  static String TELEMETRY_VIRTUAL_MEMORY = "";
  static String TELEMETRY_RESIDENT_MEMORY = "";
  static String TELEMETRY_PID = "";
  static String TELEMETRY_TIME_USED = "";
  static String TELEMETRY_LAST_UPDATED = "";

  static String TELEMETRY_TOTAL_MEMORY = "";
  static String TELEMETRY_FREE_MEMORY = "";
  static String TELEMETRY_USED_MEMORY = "";
  static String TELEMETRY_CACHE_MEMORY = "";

  static String TELEMETRY_TOTAL_SWAP = "";
  static String TELEMETRY_FREE_SWAP = "";
  static String TELEMETRY_USED_SWAP = "";
  static String TELEMETRY_AVAILABLE_MEMORY = "";

  private void
  initPrivateTelemetryStrings() {
    // need to fix this

    TELEMETRY_VIRTUAL_MEMORY = hostName + ".top_monitor.virtual_memory";
    TELEMETRY_RESIDENT_MEMORY = hostName + ".top_monitor.resident_memory";
    TELEMETRY_PID = hostName + ".top_monitor.pid";
    TELEMETRY_TIME_USED = hostName + ".top_monitor.time_used";
    TELEMETRY_LAST_UPDATED = hostName + ".top_monitor.last_updated";
    
    TELEMETRY_TOTAL_MEMORY = hostName + ".top_monitor.total_memory";
    TELEMETRY_FREE_MEMORY = hostName + ".top_monitor.free_memory";
    TELEMETRY_USED_MEMORY = hostName + ".top_monitor.used_memory";
    TELEMETRY_CACHE_MEMORY = hostName + ".top_monitor.cache_memory";

    TELEMETRY_TOTAL_SWAP = hostName + ".top_monitor.total_swap";
    TELEMETRY_FREE_SWAP = hostName + ".top_monitor.free_swap";
    TELEMETRY_USED_SWAP = hostName + ".top_monitor.used_swap";
    TELEMETRY_AVAILABLE_MEMORY = hostName + ".top_monitor.available_memory";

  }

  protected void
  doMonitor() {

    // System.out.println("doMonitor");

    TopMonitor topMonitor = new TopMonitor();

    topMonitor.initialize();
    topMonitor.getMemoryStats();

    String str;
    int pid = topMonitor.getProcessID();
    if (pid != 0)
      telemetryServer.sendTelemetry(TELEMETRY_PID, Integer.toString(pid));
    str = topMonitor.getVirtualMem();
    if (str != null)
      telemetryServer.sendTelemetry(TELEMETRY_VIRTUAL_MEMORY, str);
    str = topMonitor.getResidentMem();
    if (str != null)
      telemetryServer.sendTelemetry(TELEMETRY_RESIDENT_MEMORY, str);
    str = topMonitor.getTimeUsed();
    if (str != null)
      telemetryServer.sendTelemetry(TELEMETRY_TIME_USED, str);
    str = topMonitor.getTotalMemory();
    if (str != null)
      telemetryServer.sendTelemetry(TELEMETRY_TOTAL_MEMORY, str);
    str = topMonitor.getFreeMemory();
    if (str != null)
      telemetryServer.sendTelemetry(TELEMETRY_FREE_MEMORY, str);
    str = topMonitor.getUsedMemory();
    if (str != null)
      telemetryServer.sendTelemetry(TELEMETRY_USED_MEMORY, str);
    str = topMonitor.getCacheMemory();
    if (str != null)
      telemetryServer.sendTelemetry(TELEMETRY_CACHE_MEMORY, str);
    str = topMonitor.getTotalSwap();
    if (str != null)
      telemetryServer.sendTelemetry(TELEMETRY_TOTAL_SWAP, str);
    str = topMonitor.getFreeSwap();
    if (str != null)
      telemetryServer.sendTelemetry(TELEMETRY_FREE_SWAP, str);
    str = topMonitor.getUsedSwap();
    if (str != null)
      telemetryServer.sendTelemetry(TELEMETRY_USED_SWAP, str);
    str = topMonitor.getAvailableMemory();
    if (str != null)
      telemetryServer.sendTelemetry(TELEMETRY_AVAILABLE_MEMORY, str);

    lastMonitorRun = System.currentTimeMillis();
    telemetryServer.sendTelemetry(TELEMETRY_LAST_UPDATED,
                          lastUsedFormatter.format(lastMonitorRun));

    if (pid != storedPid) {
      // TODO:  we may just want to log this so we don't lose potential information
      if (storedPid != -1) {
        ResultsLogEntry pEntry = new ResultsLogEntry();
        String msg = "RESTART:  " + hostName +
                      " PID change:  " + storedPid +
                      " changed to " + pid;
        logger.logMessage(msg);
      }
      storedPid = pid;
    }

    ResultsLogEntry entry = new ResultsLogEntry();
    String msg = hostName +
          ": pid: " + topMonitor.getProcessID() +
          " virt: " + topMonitor.getVirtualMem() +
          " res: " + topMonitor.getResidentMem() +
          " shared: " + topMonitor.getSharedMem() +
          " time: " + topMonitor.getTimeUsed();
    logger.logMessage(msg);

    entry = new ResultsLogEntry();
    msg = hostName +
          ": mem: " + topMonitor.getTotalMemory() +
          " free: " + topMonitor.getFreeMemory() +
          " used: " + topMonitor.getUsedMemory() +
          " cache: " + topMonitor.getCacheMemory();
    logger.logMessage(msg);

    entry = new ResultsLogEntry();
    msg = hostName +
          ": swap: " + topMonitor.getTotalSwap() +
          " free: " + topMonitor.getFreeSwap() +
          " used: " + topMonitor.getUsedSwap() +
          " avail: " + topMonitor.getAvailableMemory();
    logger.logMessage(msg);

    entry = new ResultsLogEntry();
    msg = hostName +
          ": cpu user: " + topMonitor.getUserCpu() +
          " sys: " + topMonitor.getSysCpu() +
          " nice: " + topMonitor.getNiceCpu() +
          " idle: " + topMonitor.getIdleCpu();
    logger.logMessage(msg);

  }

  protected void
  dumpStatus() {

    // System.out.println("dumpStatus");

    System.out.println(dateFormatter.format(System.currentTimeMillis()));

    for (Map.Entry<String, String>entry : telemetryData.entrySet()) {
      String key = entry.getKey();
      String value = telemetryData.get(key);

      // TODO:  is the colon separated from the key?
      // System.out.println("\"" + key + "\"" + " " + "\"" + value + "\"");
      System.out.println( key + " " + value );
    }

    lastStatusDump = System.currentTimeMillis();
  }

  public void telemetryEvent(String key, String value) {
    telemetryData.put(key, value);
  }

  public void run() {

    try {
      while (!shutdownRequested) {
        sleep(sleepInterval);

        long currentTime = System.currentTimeMillis();
        if (currentTime > (lastStatusDump + statusInterval)) {
          dumpStatus();
        }
        if (currentTime > (lastMonitorRun + monitorInterval)) {
          doMonitor();
        }
      }
    } catch (InterruptedException e) {
      System.out.println("thread interrupted");
    }

  }

  public static final void main(String[] args)
      throws Exception {

    HostMonitor monitor = null;

    if (args.length > 0) {
      int port = Integer.parseInt(args[0]);
      monitor = new HostMonitor(port);
    } else {
      monitor = new HostMonitor();
    }

    monitor.initialize();
    // testing only
    monitor.initClient();
    monitor.start();

  }

}


