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
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;


public class Tail
    extends Thread {

  protected String filename = null;
  protected FileReader freader = null;
  protected BufferedReader breader = null;
  protected long interval = 0;
  protected volatile boolean shutdownRequested = false;
  protected ArrayList<TailObserver> observers = null;
  protected boolean skipToCurrent = true;
  protected volatile boolean isInitialized = false;

  public Tail(String filename) {

    this.filename = filename;
    interval = 1000L;
    observers = new ArrayList<TailObserver>();
  }

  public Tail(String filename, long interval) {
    this.filename = filename;
    this.interval = interval;
  }

  public void initialize()
      throws Exception {
    if (filename == null)
      throw new NullPointerException("filename cannot be null");

    if (isInitialized)
      return;

    freader = new FileReader(filename);

    breader = new BufferedReader(freader);

    // eventually we can make this conditional
    if (skipToCurrent)
      skipToNow();

    isInitialized = true;
  }

  public void cleanup()
    throws Exception {
    if (breader == null)
      return;

    shutdownRequested = true;

    try {
      breader.close();
    } catch (IOException e) {
      ;
    }

    breader = null;
    freader = null;

  }

  public synchronized void shutdown() {
    shutdownRequested = true;
    this.interrupt();
  }

  public synchronized void addObserver(TailObserver o) {

    if (o == null)
      return;

    observers.add(o);
  }

  public synchronized void removeObserver(TailObserver o) {
    if (o == null)
      return;

    observers.remove(o);
  }

  protected void notifyObservers(String str) {

    Iterator iter = observers.iterator();

    while (iter.hasNext()) {
      TailObserver observer = (TailObserver)iter.next();
      observer.doALine(str);
    }
  }

  public String doALine() {

    String currentLine = null;

    if (breader == null)
      return null;

    try {
      if ((currentLine = breader.readLine()) != null) {
        // System.out.println(currentLine);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    return currentLine;
  }

  protected void skipToNow() {
    String str = "start";
    
    while (str != null) {
      str = doALine();
    }

    System.out.println("skipped the beginning");
  }

  public void tailFile() {
    String line;
    boolean bool = true;

    while (bool) {
      line = doALine();
      
      // notify all our observers
      
      if (line != null) {
        System.out.println(line);
      } else {
        try {
          sleep(interval);
        } catch (InterruptedException e) {
          break;
        }
      }

      if (shutdownRequested)
        bool = false;
    }
  }

  /**
   * if you want this to be driven in its own thread, use this
   */
  public void run() {

    try {
    initialize();

    tailFile();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  /**
   * for testing purposes
   * usage:  Tail <-i interval> filename
   */
  public static void main(String[] args)
    throws Exception {

    String fn = null;
    long interval = 0;
    Tail t = null;

    if (args.length == 0) {
    } else if (args.length == 1) {
      fn = args[0];
      t = new Tail(fn);
    } else if (args.length == 3) {
      if (!args[0].equals("-i")) {
        System.err.println("usage:  java Tail <-i interval> filename");
        System.exit(1);
      }
      interval = Long.parseLong(args[1]);
      fn = args[2];
      t = new Tail(fn, interval);
    } else {
      System.err.println("usage:  java Tail <-i interval> filename");
      System.exit(1);
    }

    t.initialize();
    t.tailFile();

  }

}

