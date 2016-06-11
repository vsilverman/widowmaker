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

package test.stress;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.InputStream;
import java.io.PrintWriter;
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
    sout = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
    
  }

  public void doInteractive(InputStream in, PrintStream out)
    throws IOException
  {

    BufferedReader bin = new BufferedReader(new InputStreamReader(in));

    String line;

    for (;;) {
      // first, clean out all the output from the socket
      for (;;) {
        line = sin.readLine();
        System.out.println("line:  " + line);
        if (line == null)
          throw new IOException("unexpected EOF");
        if (line.equals(""))
          break;
        out.println(line);
      }

      String cmd = bin.readLine();
      if (cmd.equals(""))
        continue;
      sout.println(cmd);
    }
  }

  public static void main(String[] args) {
    String hostname = "localhost";
    int port = StressCmdProcessor.DEFAULT_PORT;
    String input;
    String output;

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
    client.doInteractive(System.in, System.out);
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

