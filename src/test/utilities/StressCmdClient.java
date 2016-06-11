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


/**
 * A simple client to talk to the Stress command processor. The only real
 * benefit here is that the client can take in a script and dump the output
 */

package test.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;



public class StressCmdClient
{
  private String host;
  private int port;
  private Socket socket;
  private BufferedReader sin;
  private PrintWriter sout;

  public StressCmdClient(String host,
                          int port) {
    this.host = host;
    this.port = port;
  }

  public void connect()
    throws UnknownHostException,
            IOException
  {
    socket = new Socket(host, port);
    sin = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    sout = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
    
  }

  class ReaderThread extends Thread {

    BufferedReader reader;
    PrintStream out;
    private volatile boolean isFinished = false;

    public ReaderThread(BufferedReader reader, PrintStream out) {
      this.reader = reader;
      this.out = out;
    }

    public void run() {

      String line;
      try {
        while (!isFinished) {
          // System.out.println("checking for receive traffic");
          line = sin.readLine();
          // System.out.println("line:  " + line);
          if (line == null) {
            complete();
            // throw new IOException("unexpected EOF");
          }
          else if (line.equals("")) {
            // break;
          }
          else {
            out.println(line);
            out.flush();
          }

        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public void complete() {
      isFinished = true;
    }

    public boolean isComplete() {
      return isFinished;
    }

  }

  public void doScript(String script, PrintStream out)
    throws IOException {

    if (script == null)
      return;

    File f = new File(script);
    if (!f.exists()) {
      System.err.println("StressCmdClient:  script does not exist - " + script);
      return;
    }

    FileInputStream fis = new FileInputStream(f);
    doScript(fis, out);
  }

  public void doScript(InputStream in, PrintStream out)
    throws IOException
  {

    BufferedReader bin = new BufferedReader(new InputStreamReader(in));

    String line;

    int bufSize = socket.getReceiveBufferSize();

    ReaderThread reader = new ReaderThread(sin, out);
    reader.start();

    for (;;) {
      // first, clean out all the output from the socket

      if (reader.isComplete()) {
        System.out.println("reader is finished. cleaning up.");
        break;
      }

      String cmd = null;

      while (!reader.isComplete()) {

        cmd = bin.readLine();
        if (cmd == null) {
          System.out.println("Script is complete");
          return;
        }
        sout.println(cmd);
        sout.flush();

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          System.out.println("caught InterruptedException");
        }
      }

    }

    // System.out.println("doScript is done. bye.");
  }

  public void doInteractive(InputStream in, PrintStream out)
    throws IOException
  {

    BufferedReader bin = new BufferedReader(new InputStreamReader(in));

    String line;

    int bufSize = socket.getReceiveBufferSize();

    ReaderThread reader = new ReaderThread(sin, out);
    reader.start();

    for (;;) {
      // first, clean out all the output from the socket

      if (reader.isComplete()) {
        System.out.println("reader is finished. cleaning up.");
        break;
      }

      System.out.println("waiting for a command");
      String cmd = null;

      while (!reader.isComplete()) {

        if (bin.ready()) {

          cmd = bin.readLine();
          sout.println(cmd);
          sout.flush();

        }

      }

    }

    System.out.println("doInteractive is done. bye.");
  }

  public static void main(String[] args) {
    String hostname = "localhost";
    int port = StressCmdProcessor.DEFAULT_PORT;
    String input = null;
    String output = null;

    for (int ii = 0; ii < args.length; ii++ ) {
      if (args[ii].equals("-host"))
        hostname = args[++ii];
      else if (args[ii].equals("-in"))
        input = args[++ii];
      else if (args[ii].equals("-out"))
        output = args[++ii];
      else if (args[ii].equals("-port"))
        port = Integer.parseInt(args[++ii]);
    }

    try {
    StressCmdClient client = new StressCmdClient(hostname, port);
    client.connect();
    if (input != null) {
      File f = new File(input);
      if (!f.exists()) {
        System.err.println("script file does not exist:  " + input);
        System.exit(1);
      }
      System.out.println("running script:  " + input);
      client.doScript(input, System.out);
    } else {
      client.doInteractive(System.in, System.out);
    }
    } catch (UnknownHostException e) {
      System.err.println("unknown host:  " + hostname);
      System.exit(1);
    }
    catch (IOException e) {
      System.err.println("IOException during processing");
      System.exit(1);
    }

    System.exit(0);
  }
}

