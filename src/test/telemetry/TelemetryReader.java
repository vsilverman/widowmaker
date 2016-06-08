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


package test.telemetry;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

public class
TelemetryReader
  extends Thread
{
  String host;
  int port;
  ArrayList observers;
  Socket socket = null;
  BufferedReader fromServer = null;
  PrintWriter toServer = null;
  protected Hashtable<String, String> telemetryData;

  public TelemetryReader(String host, int port) {
    this.host = host;
    this.port = port;
    observers = new ArrayList();
    telemetryData = new Hashtable<String, String>();
  }

  public void initialize()
    throws Exception
  {

    System.out.println("TelemetryReader.initialize:  connecting to " + host + " on port " + port);

    socket = new Socket(host, port);
    fromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    toServer = new PrintWriter(socket.getOutputStream());

  }

  public void shutdown() {

    try {
      if (socket != null)
        socket.close();
    } catch (IOException e) {
      // probably meaningless - do we care?
    }

  }

  public void addTelemetryObserver(TelemetryObserver observer) {

    if (observer == null)
      return;
    if (observers != null) {
      observers.add(observer);
      System.out.println("addTelemetryObserver");
    }
  }

  public void removeTelemetryObserver(TelemetryObserver observer) {

    if (observer == null)
      return;
    if (observer != null) {
      observers.remove(observer);
      System.out.println("removeTelemetryObserver");
    }
  }

  protected void sendToObservers(String line) {
    if (line == null)
      return;

    // System.out.println("sendToObservers:  line is \"" + line + "\"");

    int colon = line.indexOf(':');
    if (colon == -1) {
      System.out.println("TelemetyReader.sendToObservers:  colon was not found");
      return;
    }

    String key = line.substring(0, colon);

    // System.out.println("sendToObservers:  key token is \"" + key + "\"");

    // jump over the colon, plus the space separator
    String value = line.substring(colon+2);

    // System.out.println("stripped key is \"" + key + "\"");
    // System.out.println("stripped value is \"" + value + "\"");

    Iterator iterator = observers.iterator();
    while (iterator.hasNext()) {
      TelemetryObserver observer =
        (TelemetryObserver)iterator.next();
      observer.telemetryEvent(key, value);
    }
  }

  void handleTelemetry() {
    while (true) {
      try {
        String line = fromServer.readLine();

        // System.out.println("read " + line);


        sendToObservers(line);

      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void run() {

    handleTelemetry();

  }


  public static final void main(String[] args) {


    TelemetryReader reader = new TelemetryReader("localhost",
                                  TelemetryServer.DEFAULT_TELEMETRY_PORT);

    try {
      reader.initialize();
      reader.start();

    }
    catch (Exception e) {
      e.printStackTrace();
    }

  }

}



