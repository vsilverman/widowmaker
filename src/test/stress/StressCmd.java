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

// StressCmd.java: implementation of the StressCmd class.
//
//////////////////////////////////////////////////////////////////////

package test.stress;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class StressCmd extends Thread {
  private boolean stop = false;
  private boolean alive = true;

  public StressCmd() {
  }

  public void run() {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String command = "";
    while (alive) {
      try {
        command = in.readLine().trim();
        Thread.sleep(15000);

        if (command.equalsIgnoreCase("stop")) {
          command = "";
          stop = true;
          alive = false;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /*
   * private void commandLine(){ BufferedReader in = new BufferedReader(new
   * InputStreamReader(System.in)); String command = ""; String helpString =
   * "Please Type The Following Commands: \n" + "mem: show memory usage; \n" +
   * "gc: invoke the JVM garbage collector; \n" +
   * "stop: stop the current test; \n" + "pause: suspend all threads; \n" +
   * "continue: resume the suspended threads; \n" +
   * "? or help: to print out this help message. \n"; boolean suspended = false;
   * while (true) { if (stressGroup.activeCount() == 0) break; try { command =
   * in.readLine().trim(); } catch (java.io.IOException exc) {
   * System.out.println("IOException when reading from command line."); } if
   * (command.equalsIgnoreCase("?") || command.equalsIgnoreCase("help")) {
   * System.out.println(helpString); } else if (command.equalsIgnoreCase("mem"))
   * { System.out.println( // the heap memory usage "FreeMemory/TotalMemory   "
   * + runTime.freeMemory() + "/" + runTime.totalMemory() + "   " +
   * 100*runTime.freeMemory()/runTime.totalMemory() + "% free"); } else if
   * (command.equalsIgnoreCase("gc")) { runTime.gc(); // run garbage collection
   * } else if (command.equalsIgnoreCase("stop")) {
   * System.out.println("\nYou are about to terminate the program. \n" +
   * "Type \"Y\" to confirm. \n"); try { if(
   * (in.readLine().trim()).equalsIgnoreCase("Y") ){ // if confirmed
   * stressGroup.stop(); // stop all threads
   * System.out.println("Threads are stoped."); break; } } catch
   * (java.io.IOException exc) {;} } else if (command.equalsIgnoreCase("pause"))
   * { if (!suspended) { stressGroup.suspend(); suspended = true;
   * System.out.println("Threads are suspended."); } else
   * System.out.println("Warning: Threads are already suspended."); } else if
   * (command.equalsIgnoreCase("continue")) { if (suspended) {
   * stressGroup.resume(); suspended = false;
   * System.out.println("Threads are resumed."); } else
   * System.out.println("Warning: Threads are already running."); } else if
   * (command.equals("")) continue; else
   * System.out.println("Warning: Command not recognized!");
   * 
   * }
   * 
   * }
   */

  public boolean getStop() {
    return stop;
  }
}
