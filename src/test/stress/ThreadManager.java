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

import java.io.PrintWriter;
import java.util.StringTokenizer;

import test.utilities.StressCmdHandler;
import test.utilities.StressCmdProcessor;

public class ThreadManager
  implements StressCmdHandler {

  private static final long DEFAULT_OUTPUT_INTERVAL = 240000L;

  private StressTest[] activeThreads = null;
  private long lastOutputMillis = 0L;
  private volatile long outputInterval = DEFAULT_OUTPUT_INTERVAL;

  public ThreadManager(int MAX_THREADS) {
    activeThreads = new StressTest[MAX_THREADS];
  }

  public void initialize() {
    StressCmdProcessor processor = StressManager.cmdProcessor;
    if (processor != null)
      processor.addHandler(this);
  }

  // adds thread at the end of this vector
  public synchronized boolean addUser(ConnectionData connData, String inputFilePath, int pos) {

    for (int i = 0; i < activeThreads.length; i++) {
      if (activeThreads[i] == null) {
        System.out.println("adding user " + i);
        activeThreads[i] = new StressTest(connData, inputFilePath, i);
        activeThreads[i].start();
        try {
          // this just makes sure we don't get duplicate URIs
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // ignore it
        }

        return true;
      }
    }
    return false;
  }

  // adds a new thread based on manual input
  public synchronized boolean addCmdUser(ConnectionData connData, String inputFilePath, int pos) {

    for (int i = 0; i < activeThreads.length; i++) {
      if (activeThreads[i] == null) {
        System.out.println("adding user " + i);
        activeThreads[i] = new StressTest(connData, inputFilePath, i);
        activeThreads[i].start();
        try {
          // this just makes sure we don't get duplicate URIs
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // ignore it
        }

        return true;
      }
    }

    // if we rolled off the end, then we need to resize the threads and then add us in
    int newSize = activeThreads.length + 1;
    resizeThreads(newSize);

    // now try again - lazy approach, but let's see if it works
    for (int i = 0; i < activeThreads.length; i++) {
      if (activeThreads[i] == null) {
        System.out.println("adding user " + i);
        activeThreads[i] = new StressTest(connData, inputFilePath, i);
        activeThreads[i].start();
        try {
          // this just makes sure we don't get duplicate URIs
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // ignore it
        }

        return true;
      }
    }

    return false;
  }

  // deletes thread add pos
  // PRE: the thread at pos must have completed running
  public void deleteUser(int pos) {
    activeThreads[pos] = null;
  }

  // returns the number of threads running in this vector
  public int getCurrRunning() {
    int numRunning = 0;
    for (int i = 0; i < activeThreads.length; i++) {
      if (activeThreads[i] != null)
        if (activeThreads[i].isAlive())
          numRunning++;
    }
    return numRunning;
  }

  // returns the positions of the finished threads
  // length is the referent value to give the user
  // the length of the returned array
  public int getFinishedUser() {
    int numUsers = activeThreads.length;
    int retVal = -1;

    for (int i = 0; i < numUsers; i++) {

      if (activeThreads[i] == null) {
        retVal = i;
        i = numUsers + 1;
      } else {
        if (activeThreads[i].isAlive() == false) {
          retVal = i;
          i = numUsers + 1;
        }
      }
    }
    return retVal;
  }

  public int getDeadPosition() {
    int numUsers = activeThreads.length;
    int retVal = -1;

    // System.out.println("ThreadManager.getDeadPosition()");
    for (int i = 0; i < numUsers; i++) {
      if (activeThreads[i] != null) {
        /*
        System.out.println("getDeadPosition:  " + i + " isAlive() "
            + activeThreads[i].isAlive());
         */
        if (activeThreads[i].isAlive() == false) {
          retVal = i;
          i = numUsers + 1;
        }
      }
    }
    return retVal;
  }

  public int getOpenPosition() {
    int numUsers = activeThreads.length;
    int retVal = -1;

    for (int i = 0; i < numUsers; i++) {
      if (activeThreads[i] == null) {
        retVal = i;
        i = numUsers + 1;
      }
    }
    return retVal;

  }

  // returns the number of threads running in this vector
  public void getThreadStates(boolean forceOutput) {
    int numRunning = 0;

    long now = System.currentTimeMillis();
    if ((forceOutput == false) && ((lastOutputMillis + outputInterval) > now))
      return;

    for (int i = 0; i < activeThreads.length; i++) {
      if (activeThreads[i] != null) {
        if (activeThreads[i].isAlive()) {
			System.out.println("pos[" + i + "] alive:  " + activeThreads[i].toString());
			}
			else {
			System.out.println("pos[" + i + "] NOT alive:  " + activeThreads[i].toString());
			}
		}
		else {
			System.out.println("pos[" + i + "] is null");
		}
    }

    lastOutputMillis = now;

  }

  public synchronized void stopAllTests() {
    int length = activeThreads.length;
    for (int i = 0; i < length; i++)
      if (activeThreads[i] != null)
        activeThreads[i].setAlive(false);

  }

  /**
   * handle a command that wants a bigger thread pool than currently exists. There is no
   * harm in having a larger queue than the number of users (as far as I can read).
   */
  private synchronized void resizeThreads(int newSize) {
    // this ought to send some error some day, but not a big deal
    if (newSize <= 0)
      return;

    // nothing to do
    if (newSize <= activeThreads.length)
      return;

    StressTest[] newThreads = new StressTest[newSize];
    for (int ii = 0; ii < newSize; ii++) {
      if (ii < activeThreads.length)
        newThreads[ii] = activeThreads[ii];
      else
        newThreads[ii] = null;
    }
    activeThreads = newThreads;
  }

  public void handleCmd(String cmd, String cmdline, PrintWriter out) {

    if (cmd.equals("status")) {
      out.print("ThreadManager:\r\n");
      // we really want the thread to tell us what it is doing
      for (int i = 0; i < activeThreads.length; i++) {
        if (activeThreads[i] != null) {
          if (activeThreads[i].isAlive()) {
            out.print("pos[" + i + "] alive:  " + activeThreads[i].toString() + "\r\n");
          }
          else {
            out.print("pos[" + i + "] NOT alive:  " + activeThreads[i].toString() + "\r\n");
          }
        }
        else {
         out.print("pos[" + i + "] is null\r\n");
        }
      }
    }
    else if (cmd.equals("help")) {
      out.print("ThreadManager:\r\n");
      out.print("thread stop <thread_num>\r\n");
      out.print("thread stop all\r\n");
      out.print("output-interval <secs>\r\n");
    }
    else if (cmd.equals("thread")) {
      StringTokenizer tokens = new StringTokenizer(cmdline);
      String s = tokens.nextToken(); // trim off the initial command
      String subcmd = null;
      if (tokens.hasMoreTokens())
        subcmd = tokens.nextToken().toLowerCase();
      if (subcmd != null && subcmd.equals("stop")) {
        String t = null;
        if (tokens.hasMoreTokens())
          t = tokens.nextToken();
        out.print("stopping thread " + t + "\r\n");
        if (t != null) {
          if (t.equalsIgnoreCase("all")) {
            stopAllTests();
          }
          else {
            int thread = Integer.parseInt(t);
            if (thread < activeThreads.length) {
              activeThreads[thread].setAlive(false);
            }
          }
        }
      }
    }
    else if (cmd.equals("maxusers")) {
      StringTokenizer tokens = new StringTokenizer(cmdline);
      String s = tokens.nextToken(); // trim off the initial command
      s = tokens.nextToken();
      int newSize = Integer.parseInt(s);
      resizeThreads(newSize);
    }
    else if (cmd.equals("output-interval")) {
      StringTokenizer tokens = new StringTokenizer(cmdline);
      String s = tokens.nextToken(); // trim off the initial command
      s = tokens.nextToken();
      long newInterval = Long.parseLong(s);
      if (newInterval > 0) {
        outputInterval =  newInterval * 1000L;
      }
    }
  }

  public void setOutputInterval(int secs) {
    if (secs > 0)
      outputInterval = secs * 1000L;
  }

  public int getOutputInterval() {
    return (int)(outputInterval / 1000L);
  }

}
