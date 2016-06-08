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
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;


/**
 * Log results in a consistent format in a separate file so that they can be analyzed
 * TODO:  specify configuration later
 */


public class ResultsLogger {
  private String filename;
  private File file;
  private BufferedWriter bufferedWriter = null;
  DateFormat dateFormatter = null;

  protected static class LogFileTracker {
    private ArrayList<String> logFiles;
    private ArrayList<BufferedWriter> writers;

    private LogFileTracker() {
      logFiles = new ArrayList<String>();
      writers = new ArrayList<BufferedWriter>();
    }

    protected void add(String fname, BufferedWriter writer) {
      // do we want null to be the way to close one?
      System.out.println("tracker adding entry for " + fname);
      if (writer == null)
        System.out.println("tracker:  writer for " + fname + " is null");
      if ((fname == null) || (writer == null))
        return;
      int pos = logFiles.indexOf(fname);
      if (pos == -1) {
        logFiles.add(fname);
        writers.add(writer);
      }
      else {
        // TODO:  remove when we know this is working
        System.out.println("same logfile named used:  " + fname);
      }
    }

    protected BufferedWriter find(String fname) {
      int pos = logFiles.indexOf(fname);
      if (pos > -1)
        return writers.get(pos);
      else
        return null;
    }
  }

  private static LogFileTracker tracker = new LogFileTracker();

  public ResultsLogger() {
    dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  }

  public ResultsLogger(String fname) {
    dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    filename = fname;
  }

  public void
  init() {
    if (filename == null)
      return;
    file = new File(filename);
    if (!file.exists()) {
      try {
        file.createNewFile();
      }
      catch (Exception e) {
        System.err.println("Exception creating log file " + filename);
        e.printStackTrace();
        file = null;
      }
    }
    if (!file.canWrite()) {
      System.err.println("log file not available for write:  " + filename);
      file = null;
    } else {
      try {
        bufferedWriter = tracker.find(filename);
        if (bufferedWriter == null) {
          FileWriter fw = new FileWriter(file, true);
          bufferedWriter = new BufferedWriter(fw);
          tracker.add(filename, bufferedWriter);
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
        // just in case
        bufferedWriter = null;
      } catch (IOException e) {
        e.printStackTrace();
        // just in case
        bufferedWriter = null;
      }
    }
  }

  public void
  close() {

    try {
      if (bufferedWriter != null)
        bufferedWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    bufferedWriter = null;
    file = null;
  }

  public void
  logResult(String log) {
    Date date = new Date();
    String logStr = dateFormatter.format(date) + " - " + log;
    synchronized (bufferedWriter) {
      try {
        bufferedWriter.write(logStr);
        bufferedWriter.write("\n");
        bufferedWriter.flush();
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void
  logResult(ResultsLogEntry entry) {
    String logStr = entry.getPassFailString() + " - "
                    + Long.toString(entry.getElapsedRunTime()) + " - "
                    + Long.toString(entry.getElapsedTotalTime()) + " - "
                    + entry.getIdentifier() 
                    + ( entry.getInfo() == null ? "" : " " + entry.getInfo());

    logResult(logStr);
  }

  public void
  logMessage(String msg) {
    ResultsLogEntry entry = new ResultsLogEntry("MESSAGE: " + msg);
    entry.startTimer();
    entry.stopTimer();
    entry.setPassFail(true);
    logResult(entry);
  }

}

