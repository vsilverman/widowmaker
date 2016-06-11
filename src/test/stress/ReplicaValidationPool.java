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

/****************************************************************************
 * class ReplicaValidationPool
 * It's the driver Stress testcases.
 * 
 *****************************************************************************/

package test.stress;

import java.util.ArrayList;

public class ReplicaValidationPool extends Thread {
  private ArrayList<ValidationData> toValidate = null;
  private ValidationThread[] vThreads = null;

  public ReplicaValidationPool(int poolSize) {
    toValidate = new ArrayList<ValidationData>();
    vThreads = new ValidationThread[poolSize];
  }

  public void addValidation(ValidationData data) {
    synchronized (this) {
      // add validation data to the end of the list
      toValidate.add(data);
		// wake the queue up to be processed
		this.interrupt();
    }
  }

  private void validate() {
    boolean emptyPos = false;
    // look for empty slots in ValidationThread array
    for (int i = 0; i < vThreads.length; i++) {
      if (vThreads[i] == null)
        emptyPos = true;
      else
        emptyPos = vThreads[i].getFinished();

      if (emptyPos) {
        synchronized (this) {
          // if there are empty slots remove one of the validation data and
          // start a new thread with it
          vThreads[i] = null;
          // make sure there is actually something to validate
          if (toValidate.size() > 0) {
            // start a new validation thread with the validation data at the
            // front of the list
            vThreads[i] = new ValidationThread(toValidate.get(0));
            vThreads[i].start();
            // remove the validation data at the front o fthe list
            toValidate.remove(0);
          }
        }
      }
    }
  }

  public void run() {
    while (true) {
      try {
        while (toValidate.size() > 0)
          validate();
        Thread.sleep(5000);
      } catch (InterruptedException e) {
		    // System.out.println("ReplicaValidationPool:  queue size is " + toValidate.size());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

}
