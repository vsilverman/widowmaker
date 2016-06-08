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


package test.utilities;

import java.io.File;
import java.io.PrintStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.ArrayList;
import java.util.StringTokenizer;


/**
 * Wrapper around the mlcp command to make it easy to set options,
 * isolate invocation, and handle response
 */
public class MlcpHandler {

  public static final String IMPORT = "IMPORT";
  public static final String EXPORT = "EXPORT";

  public static final String DEFAULT_HOST = "localhost";
  public static final String TMPFILE_BASE_NAME = "MlcpOptions-";
  public static final String TMPFILE_SUFFIX = ".txt";

  public static boolean debug = false;
  public static boolean doCommand = true;
  public static boolean removeOptionsFile = true;

  protected static final Random randNum = new Random();

  protected String user;
  protected String password;
  protected String host;
  protected int port;
  protected String operation;
  protected String inputFilePath;
  protected HashMap<String, String> outputUriReplace;
  protected String outputUriPrefix;
  protected String outputCollections;
  protected String optionsFileName;
  protected int exitVal;
  protected ArrayList<String> mlcpOutput = null;
  protected int inputRecords;
  protected int outputRecords;
  protected int outputRecordsCommitted;
  protected int outputRecordsFailed;

  protected String qaHome;

  public MlcpHandler() {
    outputUriReplace = new HashMap<String, String>();
    mlcpOutput = new ArrayList<String>();
  }

  public MlcpHandler(String operation) {
    outputUriReplace = new HashMap<String, String>();
    mlcpOutput = new ArrayList<String>();
    this.operation = operation;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getInputFilePath() {
    return inputFilePath;
  }

  public void setInputFilePath(String inputFilePath) {
    this.inputFilePath = inputFilePath;
  }

  public Iterator getOutputUriReplaceKeys() {
    // do some work
    return outputUriReplace.keySet().iterator();
  }

  public String getOutputUriReplace(String key) {
    if (key == null)
      return null;

    return outputUriReplace.get(key);
  }

  public void setOutputUriReplace(String lhpattern, String rhpattern) {

    if (lhpattern == null || rhpattern == null)
      return;

    outputUriReplace.put(lhpattern, rhpattern);

  }

  public void clearOutputUriReplace() {
    outputUriReplace.clear();
  }

  public String getOutputUriPrefix() {
    return outputUriPrefix;
  }

  public void setOutputUriPrefix(String outputUriPrefix) {
    this.outputUriPrefix = outputUriPrefix;
  }

  public String getOutputCollections() {
    return outputCollections;
  }

  public void setOutputCollections(String outputCollections) {
    this.outputCollections = outputCollections;
  }

  private static String alphaChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static String numChars = "1234567890";

  protected String randomString(int len) {

    StringBuffer sb = new StringBuffer();
    
    for (int ii = 0; ii < len; ii++) {
      int pos = Math.abs(randNum.nextInt() % 26);
      sb.append(alphaChars.charAt(pos));
    }
    
    return sb.toString();
  }

  public String getQaHome() {

    if (debug)
      System.out.println("MlcpHandler.getQaHome");

    if (qaHome == null) {
      qaHome = System.getProperty("QA_HOME");
      if (debug)
        System.out.println("qaHome is null, setting to: " + qaHome);
    }

    return qaHome;
  }

  public String replaceEnvString(String str) {
    String s = str;

    String qaHome = getQaHome();
    if (qaHome != null) {
      s = str.replaceAll("QA_HOME", qaHome);
    }
    return s;
  }

  public String makeOptionsFileName() {

    String tmpdir = "/tmp";

    String tmp = System.getProperty("TMPDIR");
    if (tmp != null)
      tmpdir = tmp;

    if (tmpdir.charAt(tmpdir.length()-1) != '/') {
      tmpdir += "/";
    }

    tmpdir += TMPFILE_BASE_NAME + randomString(8) + TMPFILE_SUFFIX;

    return tmpdir;
  }

  public void generateOptionsFile(String filename) {

    if (filename == null)
      optionsFileName = "/tmp/MlcpOptions-" + randomString(8) + ".txt";
    else
      optionsFileName = filename;

    try {
      File f = new File(optionsFileName);
      PrintStream ps = new PrintStream(f);

      ps.println(operation);
      ps.println("-username");
      ps.println(user);
      ps.println("-password");
      ps.println(password);
      ps.println("-host");
      ps.println(host);
      ps.println("-port");
      ps.println(port);
      ps.println("-input_file_path");
      ps.println(inputFilePath);
      ps.println("-output_uri_replace");
      ps.println("\"" + inputFilePath + "/" + ",''\"");
      ps.println("-output_uri_prefix");
      ps.println(outputUriPrefix);
      ps.println("-output_collections");
      ps.println(outputCollections);

      ps.flush();
      ps.close();
    } catch (IOException e) {
      e.printStackTrace();
    }


  }

  public int getInputRecordCount() {
    return inputRecords;
  }

  public int getOutputRecordCount() {
    return outputRecords;
  }

  public int getOutputRecordsCommittedCount() {
    return outputRecordsCommitted;
  }

  public int getOutputRecordsFailedCount() {
    return outputRecordsFailed;
  }

  public int getExitVal() {
    return exitVal;
  }

  private void processOutput() {

    if (mlcpOutput == null)
      return;

    Iterator lines = mlcpOutput.iterator();
    while (lines.hasNext()) {
      String line = (String)lines.next();
      StringTokenizer tokens = new StringTokenizer(line);
      if (tokens.countTokens() == 0)
        continue;
      if (tokens.countTokens() < 4) {
        System.out.println("I don't know what this line is:  " + line);
        continue;
      }
      String str, str2;
      // first token is the date
      str = tokens.nextToken();
      // second token is the time
      str = tokens.nextToken();
      // third token should be INFO
      str = tokens.nextToken();
      if (!str.equals("INFO")) {
        System.out.println("This line is broken - no INFO:  " + line);
        continue;
      }
      // fourth token is LocalJobRunner
      str = tokens.nextToken();
      // fifth token is where we start figuring status out
      str = tokens.nextToken();
      if (str.contains("ContentPumpStats"))
        continue;
      // sixth token is the value for what we have
      str2 = tokens.nextToken();
      if (str.equals("INPUT_RECORDS:"))
        inputRecords = Integer.parseInt(str2);
      if (str.equals("OUTPUT_RECORDS:"))
        outputRecords = Integer.parseInt(str2);
      if (str.equals("OUTPUT_RECORDS_COMMITTED:"))
        outputRecordsCommitted = Integer.parseInt(str2);
      if (str.equals("OUTPUT_RECORDS_FAILED:"))
        outputRecordsFailed = Integer.parseInt(str2);
    }

  }

  public static final String SHELL_COMMAND = "bash -c";
  public static final String MLCP_COMMAND = "QA_HOME/mlcp/runmlcp.sh";

  public void runCommand() {

    if (debug)
      System.out.println("MlcpHandler.runCommand");

    StringBuffer cmdBuf = new StringBuffer();

    if (optionsFileName == null)
      generateOptionsFile(null);

    String cmd = replaceEnvString(MLCP_COMMAND);

    cmdBuf.append(cmd);
    cmdBuf.append(" ");
    cmdBuf.append(getQaHome());
    cmdBuf.append(" ");
    cmdBuf.append("-options_file");
    cmdBuf.append(" ");
    cmdBuf.append(optionsFileName);

    if (debug)
      System.out.println("Command to be run:  " + cmdBuf.toString());

    StringBuilder results = null;

    if (doCommand) {
      results = runCmd(cmdBuf.toString());

      processOutput();

      if (debug) {
        System.out.println("results from command:");
        System.out.println(results);
      }
    }

/*
    String results = System.exec(cmdBuf.toString());
*/

  }


  protected StringBuilder runCmd(String cmd) {
    StringBuilder sb = new StringBuilder();
    BufferedReader br = null;
    Runtime r = Runtime.getRuntime();
    InputStream errStream;
    InputStreamReader esr;

    if (debug)
      System.out.println("MlcpHandler.runCmd");

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

      if (debug)
        System.out.println("<ERROR>");
      while ((errline = ebr.readLine()) != null) {
        if (debug)
          System.out.println(errline);
        mlcpOutput.add(errline);
      }
      if (debug)
        System.out.println("</ERROR>");

      exitVal = p.waitFor();
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

  public void cleanup() {

    if (removeOptionsFile) {
      File f = new File(optionsFileName);
      f.delete();
    }

  }


/*
IMPORT
-username
admin
-password
admin
-host
jjames-z620
-port
5275
-input_file_path
/tmp/RDRVFRYPMPECCDPO
-output_uri_replace
"/tmp/RDRVFRYPMPECCDPO/,''"
-output_uri_prefix
/MNCFOHWVMWVWTMYO/2016424155123200/0/
-output_collections
/MNCFOHWVMWVWTMYO/2016424155123200/0/
*/

}

