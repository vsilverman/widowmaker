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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import test.telemetry.TelemetryObserver;
import test.telemetry.TelemetryReader;

public class ClusterMonitor
    extends Thread
    implements TelemetryObserver {

  public static final long DEFAULT_SLEEP_INTERVAL = 60000L;

  boolean bDebug = false;
  volatile boolean shutdownRequested = false;
  long sleepInterval = DEFAULT_SLEEP_INTERVAL;
  String logfile = null;
  HashMap<String, TelemetryReader> hostMonitors = null;
  HashMap<String, HostWrapper> wrappers = null;

  class HostWrapper
      implements TelemetryObserver {
    String host;
    int port;
    TelemetryReader reader;
    HashMap<String, String> details = null;

    HostWrapper(String host, int port, TelemetryReader reader) {
      this.host = host;
      this.port = port;
      this.reader = reader;
      details = new HashMap<String, String>();
    }

    static final String MONITOR_VIRTUAL_MEM_SUFFIX = ".top_monitor.virtual_memory";
    static final String MONITOR_RESIDENT_MEM_SUFFIX = ".top_monitor.resident_memory";
    static final String MONITOR_PID_SUFFIX = ".top_monitor.pid";
    static final String MONITOR_TIME_USED_SUFFIX = ".top_monitor.time_used";
    static final String MONITOR_LAST_UPDATED_SUFFIX = ".top_monitor.last_updated";
  
    String MONITOR_VIRTUAL_MEM;
    String MONITOR_RESIDENT_MEM;
    String MONITOR_PID;
    String MONITOR_TIME_USED;
    String MONITOR_LAST_UPDATED;

    public void initialize() {
      MONITOR_VIRTUAL_MEM = host+MONITOR_VIRTUAL_MEM_SUFFIX;
      MONITOR_RESIDENT_MEM = host+MONITOR_RESIDENT_MEM_SUFFIX;
      MONITOR_PID = host+MONITOR_PID_SUFFIX;
      MONITOR_TIME_USED = host+MONITOR_TIME_USED_SUFFIX;
      MONITOR_LAST_UPDATED = host+MONITOR_LAST_UPDATED_SUFFIX;

      details.put(MONITOR_VIRTUAL_MEM, null);
      details.put(MONITOR_RESIDENT_MEM, null);
      details.put(MONITOR_PID, null);
      details.put(MONITOR_TIME_USED, null);
      details.put(MONITOR_LAST_UPDATED, null);
    }

    private boolean isMonitoredEvent(String key) {
      Iterator iterator = details.keySet().iterator();
      while (iterator.hasNext()) {
        String str = (String)iterator.next();
        if (str.equals(key))
          return true;
      }
      return false;
    }

    public void telemetryEvent(String key, String value) {

      // System.out.println("telemetryEvent:  " + key);
      if (key.startsWith(host)) {
        // System.out.println("looking through the entries");
        if (isMonitoredEvent(key))
          details.put(key, value);
      }
    }

    public void logOutput() {

      String virtMem = details.get(MONITOR_VIRTUAL_MEM);
      String resMem = details.get(MONITOR_RESIDENT_MEM);
      String pid = details.get(MONITOR_PID);
      String timeUsed = details.get(MONITOR_TIME_USED);
      String lastUpdated = details.get(MONITOR_LAST_UPDATED);

      String str = "host " + host +
                    " pid: " + pid +
                    " virt: " + virtMem +
                    " res: " + resMem +
                    " time: " + timeUsed;

      System.out.println(str);

      // logger.logMessage(str);
    }

  }

  public ClusterMonitor() {

    hostMonitors = new HashMap<String, TelemetryReader>();
    wrappers = new HashMap<String, HostWrapper>();
  }

  public void initialize() {

    // for each host in the cluster
    // connect a reader to the host
    // place it in the map

    // String host = "jjames-z620";
    String host = "localhost";
    int port = HostMonitor.DEFAULT_TELEMETRY_PORT;

    TelemetryReader reader = new TelemetryReader(host, port);
    HostWrapper wrapper = new HostWrapper(host, port, reader);
    wrapper.initialize();
    try {
    reader.initialize();

    reader.addTelemetryObserver(this);
    reader.addTelemetryObserver(wrapper);

    reader.start();

    hostMonitors.put(host, reader);

      wrappers.put(host, wrapper);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void shutdown() {

    shutdownRequested = true;

  }

  protected void logOutput() {

    for (Map.Entry entry : wrappers.entrySet()) {
      HostWrapper wrapper = (HostWrapper)entry.getValue();
      wrapper.logOutput();

    }

  }

  public void run() {

    while (!shutdownRequested) {
      try {
        sleep(sleepInterval);
        logOutput();
      } catch (InterruptedException e) {
        // do nothing
      }

    }

  }

  public void telemetryEvent(String key, String value) {

    // System.out.println(key + " " + value);
  }

  public static void main(String[] args) {

    ClusterMonitor monitor = new ClusterMonitor();

    monitor.initialize();
    monitor.start();

    try {
    monitor.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

}

