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


package test.stress;

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

  void addTelemetryObserver(TelemetryObserver observer) {

    if (observer == null)
      return;
    if (observers != null) {
      observers.add(observer);
      System.out.println("addTelemetryObserver");
    }
  }

  void removeTelemetryObserver(TelemetryObserver observer) {

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

    StringTokenizer tokens = new StringTokenizer(line);

    if (tokens.countTokens() < 2) {
      System.out.println("TelemetyReader.sendToObservers:  token count was "
                    + tokens.countTokens());
      return;
    }

    String key = tokens.nextToken();
    // we have to do this because date strings have spaces - bummer
    String value = line.substring(key.length());


    Iterator iterator = observers.iterator();
    while (iterator.hasNext()) {
      TelemetryObserver observer =
        (TelemetryObserver)iterator.next();
      // System.out.println("sendToObservers:  " + key + ", " + value);
      observer.telemetryEvent(key, value);
    }
  }

  void handleTelemetry() {
    while (true) {
      try {
        String line = fromServer.readLine();

        // System.out.println(line);


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



