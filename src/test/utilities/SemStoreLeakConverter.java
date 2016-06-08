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
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.StringTokenizer;

import org.joda.time.DateTime;

/**
 * Quick and dirty class to take the inference store allocation and de-allocation
 * log entries from an ErrorLog.txt file and convert them into triples that can be
 * later searched to look for store memory leaks
 */
public class SemStoreLeakConverter
{
  String infilename;
  String outfilename;

  public SemStoreLeakConverter(String infile, String outfile) {
    infilename = infile;
    outfilename = outfile;
  }

  void writeProlog(PrintStream ps)
  {

    ps.println("");
    ps.println("prefix xsd:  <http://www.w3.org/2001/XMLSchema#>");
    ps.println("prefix rdf:  <http://www.w3.org/1999/22-rdf-syntax-ns#>");
    ps.println("prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#>");
    ps.println("prefix tstore: <http://marklogic.com/semantics/inference/alloc#>");
    ps.println("");

    ps.println("INSERT DATA");
    ps.println("{");

    ps.println("GRAPH  <TripleStoreAlloc>");
    ps.println("{");
  }

  void writeEpilog(PrintStream ps)
  {
    ps.println("");
    ps.println("}"); // close graph
    ps.println("}");  // close insert data
    ps.println("");
  }

  void processLine(String line, PrintStream ps)
    throws Exception {

    if (line == null)
      return;

    StringTokenizer tokens = new StringTokenizer(line);
    if (tokens.countTokens() == 0)
      return;

    String dstr = tokens.nextToken();
    String tstr = tokens.nextToken();

    String datestr = dstr + "T" + tstr;

    DateTime dtime = new DateTime(datestr);

    // peel off Info:
    String info = tokens.nextToken();
    // peel off [Event
    String event = tokens.nextToken();
    // grab hold of operation with trailing ]
    String operation = tokens.nextToken();
    int bracket = operation.indexOf(']');
    if (bracket > 0)
      operation = operation.substring(0, bracket);

    // type of TripleStore in hand
    String storeType = tokens.nextToken();

    // the operation again
    String op = tokens.nextToken();

    // the address
    String addrStr = tokens.nextToken();
    int eq = addrStr.indexOf('=');
    if (eq > 0)
      addrStr = addrStr.substring(eq+1);

    String memsize = null;
    // if it's a created line, we might have a trailing , and a mem size out there
    if (op.equals("created:")) {
      int comma = addrStr.indexOf(',');
      if (comma > 0)
        addrStr = addrStr.substring(0, comma);
      try {
        memsize = tokens.nextToken();
        if (memsize != null) {
          eq = memsize.indexOf('=');
          if (eq > 0)
            memsize = memsize.substring(eq+1);
        }
      }
      catch (ArrayIndexOutOfBoundsException e) {
      }
    }

    int month = dtime.monthOfYear().get();
    int day = dtime.monthOfYear().get();
    int hour = dtime.hourOfDay().get();
    int minute = dtime.minuteOfHour().get();
    int seconds = dtime.secondOfMinute().get();
    int millis = dtime.millisOfSecond().get();

    // construct a key
    String subject = "tstore:" + 
        dtime.year().getAsText() +
        ( month < 10 ? "0" + month : month) +
        ( day < 10 ? "0" + day : day) +
        ( hour < 10 ? "0" + hour : hour) +
        ( minute < 10 ? "0" + minute : minute) +
        ( seconds < 10 ? "0" + seconds : seconds) +
        "." +
        ( millis < 100 ? "0" : "" ) +
        ( millis < 10 ? "0" + millis : millis) +
        "_" + operation;

    ps.println(subject + "\t" + "tstore:timestamp" + "\t\"" + dtime.toLocalDateTime() + "\"^^xsd:dateTime .");
    ps.println(subject + "\t" + "tstore:type" + "\t\"" + operation + "\" .");
    ps.println(subject + "\t" + "tstore:address" + "\t\"" + addrStr + "\" .");
    if (memsize != null)
      ps.println(subject + "\t" + "tstore:memsize" + "\t\"" + memsize + "\"^^xsd:integer .");
    ps.println("");

  }

  public void processFile()
    throws Exception {
    File infile = null;
    File outfile = null;
    BufferedReader br = null;
    PrintStream ps = null;

    // for now, get out of here, but we really want to take the input from stdin
    if (infilename == null) {
      throw new Exception("input filename is null");
    }

    // for now, get out of here, but we really want to send output to stdout
    if (outfilename == null) {
      throw new Exception("output filename is null");
    }

    try {
      infile = new File(infilename);
      br = new BufferedReader(new FileReader(infile));
    }
    catch (FileNotFoundException e) {
      throw new Exception("file does not exist:  " + infilename);
    }

    try {
      outfile = new File(outfilename);
      ps = new PrintStream(new FileOutputStream(outfile));
    }
    catch (FileNotFoundException e) {
      throw new Exception("file does not exist:  " + outfilename);
    }

    writeProlog(ps);

    String line = null;
    while ((line = br.readLine()) != null) {
      processLine(line, ps);
    }

    writeEpilog(ps);
    ps.flush();
  }

  public static final void main(String[] args) {

      String in = args[0];
      String out = args[1];

      SemStoreLeakConverter logger = new SemStoreLeakConverter(in, out);
      try {
      logger.processFile();
      } catch (Exception e) {
        e.printStackTrace();
      }

  }

}


