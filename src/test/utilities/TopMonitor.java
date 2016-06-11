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


package test.utilities;

import java.io.File;
import java.io.PrintWriter;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.StringTokenizer;


public class TopMonitor {

  public static final String DEFAULT_PROCESS_NAME = "MarkLogic";

  protected int pid;
  protected String procName;
  protected String virtualMem;
  protected String residentMem;
  protected String sharedMem;
  protected String timeUsed;
  // memory stats from top
  protected String totalMemory;
  protected String freeMemory;
  protected String usedMemory;
  protected String cacheMemory;
  protected String memBuffers;
  // swap stats from top
  protected String totalSwap;
  protected String freeSwap;
  protected String usedSwap;
  protected String availableMemory;

  // cpu stats
  protected String totalUserCpu;
  protected String totalSysCpu;
  protected String totalNiceCpu;
  protected String totalIdleCpu;
  protected String totalWaCpu;
  protected String totalHiCpu;
  protected String totalSiCpu;
  protected String totalStCpu;
  
  protected boolean debug = false;

  protected String psAwkFilename;
  protected String topAwkFilename;

  public TopMonitor() {
    procName = DEFAULT_PROCESS_NAME;
    debug = false;
    pid = 0;
    virtualMem = "";
    residentMem = "";
    sharedMem = "";
    timeUsed = "";
    totalMemory = "";
    freeMemory = "";
    usedMemory = "";
    cacheMemory = "";
    totalSwap = "";
    freeSwap = "";
    usedSwap = "";
    availableMemory = "";
  }

  public TopMonitor(String procName) {
    this();
    this.procName = procName;
  }

  public TopMonitor(int pid) {
    this();
    this.pid = pid;
  }

  protected void makeTempFiles() {

    String tmpdir = System.getProperty("java.io.tmpdir");
    String separator = System.getProperty("file.separator");

    // System.out.println("tmpdir is " + tmpdir);
    // System.out.println("separator is " + separator);

    if (!tmpdir.endsWith(separator)) {
      tmpdir = tmpdir + separator;
    }

    topAwkFilename = tmpdir + "top-output.awk";

    File topFile = new File(topAwkFilename);

    if (!topFile.exists()) {
      try {
        PrintWriter ps = new PrintWriter(topFile);
        // ps.open();
        ps.println("BEGIN { FS = \" \" }");
        ps.println("{");
        ps.println("printf(\"virtual:  %s  resident:  %s  shared:  %s  time:  %s\\n\", $5, $6, $7, $11)");
        ps.println("}");
        ps.flush();
        ps.close();
      }
      catch (FileNotFoundException e) {
        System.err.println("creating top-output.awk file");
        e.printStackTrace();
      }
    } else {
      if (debug)
        System.out.println("top file already exists");
    }

    psAwkFilename = tmpdir + "ps-output.awk";

    File psFile = new File(psAwkFilename);

    if (!psFile.exists()) {
      try {
      PrintWriter ps = new PrintWriter(psFile);
      // ps.open();
      ps.println("BEGIN { FS = \" \" }");
      ps.println("{");
      ps.println("if ($1 == \"daemon\")");
      ps.println("print $2");
      ps.println("endif");
      ps.println("}");
      ps.flush();
      ps.close();
      }
      catch (FileNotFoundException e) {
        System.err.println("creating ps-output.awk file");
        e.printStackTrace();
      }
    } else {
      if (debug)
        System.out.println("ps file already exists");
    }

  }

  public void initialize() {
    makeTempFiles();
  }

  protected StringBuilder runCmd(String cmd) {
    StringBuilder sb = new StringBuilder();
    BufferedReader br = null;
    Runtime r = Runtime.getRuntime();
    InputStream errStream;
    InputStreamReader esr;

    String[] cmdArray = { "/bin/bash", "-c", cmd };

    try {

      if (debug) {
        System.out.println("about to run command " + cmd);
      }

      Process p = r.exec(cmdArray);
      errStream = p.getErrorStream();
      esr = new InputStreamReader(errStream);
      BufferedReader ebr = new BufferedReader(esr);
      String errline = "";
      if (debug) {
        System.out.println("<ERROR>");
        while ((errline = ebr.readLine()) != null) {
          System.out.println(errline);
        }
        System.out.println("</ERROR>");
      }

      int exitVal = p.waitFor();
      if (debug) {
        System.out.println("command returned " + exitVal);
        System.out.println("command finished");
      }

      br = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line = "";

      while ((line = br.readLine()) != null) {
        sb.append(line);
      }

    }
    catch (IOException e) {
      e.printStackTrace();
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
    finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e2) {
          // eventually we'll just ignore this
          e2.printStackTrace();
        }
      }
    }

    return sb;
  }

  protected int findPid() {

    String cmd = "/usr/bin/ps -ef | /usr/bin/grep " + procName + " | /usr/bin/awk -f "
                      + psAwkFilename;


    StringBuilder result = runCmd(cmd);
    String str = "";
    if (result != null && result.length() > 0) {
      str = result.toString();
      pid = Integer.parseInt(str);
      if (debug) {
        System.out.println("result was " + str);
      }

    }

    return pid;
  }

  public int getPid() {
    if (pid == 0)
      pid = findPid();

    return pid;
  }

  private void getMemStats2(String line) {
    int pos;
    int startPos, endPos;
    String tmpVal;
    String tmp;

    if ((line == null) || (line.length() == 0))
      return;

    // KiB Mem : 14837385+total, 60484928 free, 25000940 used, 62887988 buff/cache

    // brute force parsing because of the potential embedded + symbols for some values
    pos = 0;
    // move past the KiB Mem
    pos += 7;
    // move past any whitespace between 'Mem' and ':'
    while ((pos < line.length()) && (Character.isWhitespace(line.charAt(pos))))
      ++pos;
    // move past the colon
    while ((pos < line.length()) && (line.charAt(pos) == ':'))
      ++pos;
    // move past any whitespace following ':'
    while ((pos < line.length()) && (Character.isWhitespace(line.charAt(pos))))
      ++pos;

    // should be pointing at the first digit
    startPos = pos;
    while ((pos < line.length()) && (Character.isDigit(line.charAt(pos))))
      ++pos;
    // we might be at a '+'
    while ((pos < line.length()) && (line.charAt(pos) == '+'))
      ++pos;
    // now we're pointed at a whitespace character
    endPos = pos;

    tmp = line.substring(startPos, endPos);

    totalMemory = tmp;

    // move past any whitespace 
    while ((pos < line.length()) && (Character.isWhitespace(line.charAt(pos))))
      ++pos;
    //
    // we should now be at 'total'
    if (!(line.substring(pos, pos+5).equals("total")))
      System.out.println("we're lost:  " + line.substring(pos, pos+5));
    else
      pos += 5;

    // move past the ','
    while ((pos < line.length()) && (line.charAt(pos) == ','))
      ++pos;
    // move past whitespace
    while ((pos < line.length()) && (Character.isWhitespace(line.charAt(pos))))
      ++pos;

    startPos = pos;
    while ((pos < line.length()) && (Character.isDigit(line.charAt(pos))))
      ++pos;
    endPos = pos;
    tmp = line.substring(startPos, endPos);
    // we don't know what this is yet
    tmpVal = tmp;

    // move past the whitespace
    ++pos;
    while ((pos < line.length()) && (Character.isWhitespace(line.charAt(pos))))
      ++pos;
    if (line.substring(pos, pos+4).equals("free")) {
      pos += 4;
      freeMemory = tmpVal;
    } else if (line.substring(pos, pos+4).equals("used")) {
      pos += 4;
      usedMemory = tmpVal;
    } else {
      System.out.println("We're totally lost:  " + line.substring(pos));
    }

    // move past the ','
    while ((pos < line.length()) && (line.charAt(pos) == ','))
      ++pos;
    // move past whitespace
    while ((pos < line.length()) && (Character.isWhitespace(line.charAt(pos))))
      ++pos;

    startPos = pos;
    while ((pos < line.length()) && (Character.isDigit(line.charAt(pos))))
      ++pos;
    endPos = pos;
    tmp = line.substring(startPos, endPos);
    // we don't know what this is yet
    tmpVal = tmp;

    // move past the whitespace
    ++pos;
    while ((pos < line.length()) && (Character.isWhitespace(line.charAt(pos))))
      ++pos;
    if (line.substring(pos, pos+4).equals("free")) {
      pos += 4;
      freeMemory = tmpVal;
    } else if (line.substring(pos, pos+4).equals("used")) {
      pos += 4;
      usedMemory = tmpVal;
    } else {
      System.out.println("We're totally lost:  " + line.substring(pos));
    }

    while ((pos < line.length()) &&
              ((line.charAt(pos) == '.') || (line.charAt(pos) == ',')))
      ++pos;

    while ((pos < line.length()) && Character.isWhitespace(line.charAt(pos)))
      ++pos;


    // now to get the avail on the end
    startPos = pos;
    while ((pos < line.length()) && (Character.isDigit(line.charAt(pos))))
      ++pos;
    endPos = pos;
    tmp = line.substring(startPos, endPos);
    memBuffers = tmp;
    cacheMemory = tmp;

  }

  private void getMemStats1(String line) {
    StringTokenizer tokenizer = null;
    String cmd;
    StringBuilder result;
    String str;

    tokenizer = new StringTokenizer(line);
    if (tokenizer.countTokens() == 11) {
      String s;
      s = tokenizer.nextToken();  // KiB
      s = tokenizer.nextToken();  // Mem
      s = tokenizer.nextToken();  // :
      totalMemory = tokenizer.nextToken();  // total mem
      s = tokenizer.nextToken();  // total,
      freeMemory = tokenizer.nextToken();
      s = tokenizer.nextToken();  // free,
      usedMemory = tokenizer.nextToken();
      s = tokenizer.nextToken();  // used,
      cacheMemory = tokenizer.nextToken();
    } else {
      System.out.println("getMemStats1:  wrong number of tokens:  " + line);
    }

  }

  protected void getMemStats() {
    StringTokenizer tokenizer = null;
    String cmd;
    StringBuilder result;
    String str;

    cmd = "top -b -n 1 | grep KiB | grep buff";
    result = runCmd(cmd);
    str = "";
    if (result != null && result.length() > 0) {
      str = result.toString();
      if (debug) {
        System.out.println("result was " + str);
      }
    }

    if (str.contains("buffers")) {
      getMemStats2(str);
    } else if (str.contains("cache")) {
      getMemStats2(str);
    } else {
      System.out.println("getMemStats:  we're totally lost:  " + str);
    }

  }


  protected void getSwapStats2(String line) {
    StringTokenizer tokenizer = null;
    int pos;
    int startPos, endPos;
    String tmpVal;
    String tmp;

    if ((line == null) || (line.length() == 0))
      return;

    // KiB Swap: 67108860 total,        0 used, 67108860 free, 13793508 cached
    tokenizer = new StringTokenizer(line);
    if (tokenizer.countTokens() == 10) {
      String s;
      s = tokenizer.nextToken();  // KiB
      s = tokenizer.nextToken();  // Swap:
      totalSwap = tokenizer.nextToken();  // total mem
      s = tokenizer.nextToken();  // total,
      usedSwap = tokenizer.nextToken();
      s = tokenizer.nextToken();  // used,
      freeSwap = tokenizer.nextToken();
      s = tokenizer.nextToken();  // free,
      cacheMemory = tokenizer.nextToken();
    } else {
      System.out.println("getSwapStats2:  wrong number of tokens:  " + line);
    }

  }


  protected void getSwapStats1(String line) {
    int pos;
    int startPos, endPos;
    String tmpVal;
    String tmp;

    if ((line == null) || (line.length() == 0))
      return;

    // KiB Swap:  8388604 total,  8388604 free,        0 used. 12234953+avail Mem

    // brute force parsing because of the potential embedded + symbols for some values
    pos = 0;
    // move past the KiB Swap:
    pos += 9;
    while ((pos < line.length()) && (Character.isWhitespace(line.charAt(pos))))
      ++pos;

    // should be pointing at the first digit
    startPos = pos;
    while ((pos < line.length()) && (Character.isDigit(line.charAt(pos))))
      ++pos;
    // now we're either pointed at whitespace, or we're pointed at a '+' character
    endPos = pos;
    tmp = line.substring(startPos, endPos);
    totalSwap = tmp;

    ++pos;
    // we should now be at 'total'
    if (!(line.substring(pos, pos+5).equals("total")))
      System.out.println("we're lost:  " + line.substring(pos, pos+5));
    else
      pos += 5;

    // move past the ','
    ++pos;
    while ((pos < line.length()) && (Character.isWhitespace(line.charAt(pos))))
      ++pos;
    startPos = pos;
    while ((pos < line.length()) && (Character.isDigit(line.charAt(pos))))
      ++pos;
    endPos = pos;
    tmp = line.substring(startPos, endPos);
    // we don't know what this is yet
    tmpVal = tmp;

    // move past the whitespace
    ++pos;
    while ((pos < line.length()) && (Character.isWhitespace(line.charAt(pos))))
      ++pos;
    if (line.substring(pos, pos+4).equals("free")) {
      pos += 4;
      freeSwap = tmpVal;
    } else if (line.substring(pos, pos+4).equals("used")) {
      pos += 4;
      usedSwap = tmpVal;
    } else {
      System.out.println("We're totally lost:  " + line.substring(pos));
    }

    while ((pos < line.length()) &&
              ((line.charAt(pos) == '.') || (line.charAt(pos) == ',')))
      ++pos;

    while ((pos < line.length()) && Character.isWhitespace(line.charAt(pos)))
      ++pos;

    startPos = pos;
    while ((pos < line.length()) && (Character.isDigit(line.charAt(pos))))
      ++pos;
    endPos = pos;
    tmp = line.substring(startPos, endPos);
    // we don't know what this is yet
    tmpVal = tmp;

    // move past the whitespace
    ++pos;
    while ((pos < line.length()) && (Character.isWhitespace(line.charAt(pos))))
      ++pos;
    if (line.substring(pos, pos+4).equals("free")) {
      pos += 4;
      freeSwap = tmpVal;
    } else if (line.substring(pos, pos+4).equals("used")) {
      pos += 4;
      usedSwap = tmpVal;
    } else {
      System.out.println("We're totally lost:  " + line.substring(pos));
    }

    while ((pos < line.length()) &&
              ((line.charAt(pos) == '.') || (line.charAt(pos) == ',')))
      ++pos;

    while ((pos < line.length()) && Character.isWhitespace(line.charAt(pos)))
      ++pos;

    // now to get the avail on the end
    startPos = pos;
    while ((pos < line.length()) && (Character.isDigit(line.charAt(pos))))
      ++pos;
    while ((pos < line.length()) && (line.charAt(pos) == '+'))
      ++pos;
    endPos = pos;
    tmp = line.substring(startPos, endPos);
    availableMemory = tmp;

  }

  protected void getSwapStats() {
    StringTokenizer tokenizer = null;
    String cmd;
    String str;
    StringBuilder result;

    cmd = "top -b -n 1 | grep KiB | grep Swap";
    result = runCmd(cmd);
    str = "";
    if (result != null && result.length() > 0) {
      str = result.toString();
      if (debug) {
        System.out.println("result was " + str);
      }
    }

    if (str.contains("cache"))
      getSwapStats2(str);
    else
      getSwapStats1(str);

  }

  private void getCpuStats1(String line) {
    StringTokenizer tokenizer = null;

    if (line == null)
      return;
    if (line.length() == 0)
      return;

    // %Cpu(s):  6.8 us,  0.8 sy,  0.0 ni, 92.4 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st

    tokenizer = new StringTokenizer(line);
    if (tokenizer.countTokens() == 17) {
      String s;
      s = tokenizer.nextToken();  // %Cpu(s)
      totalUserCpu = tokenizer.nextToken();  // user
      s = tokenizer.nextToken();  // us,
      totalSysCpu = tokenizer.nextToken();
      s = tokenizer.nextToken();  // sy,
      totalNiceCpu = tokenizer.nextToken();
      s = tokenizer.nextToken();  // ni,
      totalIdleCpu = tokenizer.nextToken();
      s = tokenizer.nextToken();  // id,
      totalWaCpu = tokenizer.nextToken();
      s = tokenizer.nextToken();  // wa,
      totalHiCpu = tokenizer.nextToken();
      s = tokenizer.nextToken();  // hi,
      totalSiCpu = tokenizer.nextToken();
      s = tokenizer.nextToken();  // si
      totalStCpu = tokenizer.nextToken();
      s = tokenizer.nextToken();  // st
    } else {
      System.out.println("getCpuStats1:  wrong number of tokens:  " + line);
    }

  }

  protected void getCpuStats() {
    String cmd;
    StringBuilder result;
    String str;

      //%Cpu(s):  5.5 us,  0.2 sy,  0.0 ni, 94.1 id,  0.0 wa,  0.1 hi,  0.1 si,  0.0 st

    cmd = "top -b -n 1 | grep Cpu";
    result = runCmd(cmd);
    str = "";
    if (result != null && result.length() > 0) {
      str = result.toString();
      if (debug) {
        System.out.println("result was " + str);
      }
    }

    if (str.substring(0, 8).equals("%Cpu(s):"))
      getCpuStats1(str);

  }

  public void getMemoryStats() {
    int pid = getPid();

    if (debug) {
      System.out.println(procName + " pid is " + pid);
    }

    String cmd = "top -b -n 1 | grep " + pid + " | awk -f " + topAwkFilename;

    StringBuilder result = runCmd(cmd);
    String str = "";
    if (result != null && result.length() > 0) {
      str = result.toString();
      if (debug) {
        System.out.println("result was " + str);
      }
    }

    StringTokenizer tokenizer = new StringTokenizer(str);
    if (tokenizer.countTokens() == 8) {
      String s = tokenizer.nextToken();
      virtualMem = tokenizer.nextToken();
      s = tokenizer.nextToken();
      residentMem = tokenizer.nextToken();
      s = tokenizer.nextToken();
      sharedMem = tokenizer.nextToken();
      s = tokenizer.nextToken();
      timeUsed = tokenizer.nextToken();

    } else {
      System.out.println("getMemoryStats:  wrong number of tokens:  " + str);
    }

    getMemStats();
    getSwapStats();
    getCpuStats();

  }

  public int getProcessID() {
    return pid;
  }

  public String getVirtualMem() {
    return virtualMem;
  }

  public String getResidentMem() {
    return residentMem;
  }

  public String getSharedMem() {
    return sharedMem;
  }

  public String getTimeUsed() {
    return timeUsed;
  }

  public String getTotalMemory() {
    return totalMemory;
  }

  public String getFreeMemory() {
    return freeMemory;
  }

  public String getUsedMemory() {
    return usedMemory;
  }

  public String getCacheMemory() {
    return cacheMemory;
  }

  public String getTotalSwap() {
    return totalSwap;
  }

  public String getFreeSwap() {
    return freeSwap;
  }

  public String getUsedSwap() {
    return usedSwap;
  }

  public String getAvailableMemory() {
    return availableMemory;
  }

  public String getUserCpu() {
    return totalUserCpu;
  }

  public String getSysCpu() {
    return totalSysCpu;
  }

  public String getNiceCpu() {
    return totalNiceCpu;
  }

  public String getIdleCpu() {
    return totalIdleCpu;
  }

  public void setDebug(boolean d) {
    debug = d;
  }

  public boolean getDebug() {
    return debug;
  }

  public static final void main(String[] args) {

    TopMonitor mem = new TopMonitor();

    if (args.length > 0) {
      if (args[0].equals("-d"))
        mem.setDebug(true);
    }

    mem.initialize();
    mem.getMemoryStats();
    System.out.println("Process ID:  " + mem.getProcessID());
    System.out.println("Virtual memory:  " + mem.getVirtualMem());
    System.out.println("Resident memory:  " + mem.getResidentMem());
    System.out.println("Shared memory:  " + mem.getSharedMem());
    System.out.println("Processing Time:  " + mem.getTimeUsed());
    System.out.println("System memory stats:");
    System.out.println("Total Memory:  " + mem.getTotalMemory());
    System.out.println("Free Memory:  " + mem.getFreeMemory());
    System.out.println("Used Memory:  " + mem.getUsedMemory());
    System.out.println("Cache Memory:  " + mem.getCacheMemory());
    System.out.println("Total Swap:  " + mem.getTotalSwap());
    System.out.println("Free Swap:  " + mem.getFreeSwap());
    System.out.println("Used Swap:  " + mem.getUsedSwap());
    System.out.println("Available Memory:  " + mem.getAvailableMemory());
  }

}

