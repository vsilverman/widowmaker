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

/****************************************************************************
 * class ValidationThread
 * 
 *****************************************************************************/

package test.stress;

import java.io.Reader;
import java.math.BigInteger;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;

public class ValidationThread extends Thread {
  private ConnectionData connectionData = null;
  private String xqStr = null;
  private String expectedValue = null;
  private String tStamp = null;
  private Session session = null;
  private boolean finished = false;
  private int numTries = 0;
  private int serverIndex = 0;
  // TODO:  can probably just retain the notifier?? will decide when lag testing continues
  private ValidationData vData = null;

  public ValidationThread(ValidationData validationData) {
    connectionData = validationData.getConnectionData();
    xqStr = validationData.getValidationQuery();
    expectedValue = validationData.getExpectedValue();
    tStamp = validationData.getTimeStamp();
    serverIndex = validationData.getServerIndex();
  vData = validationData;
  }

  public boolean getFinished() {
    return finished;
  }

  public static String getValueFromResults(ResultSequence resultSeq)
      throws Exception {
    StringBuffer resultsBuffer = new StringBuffer();
    try {
      char[] readBuffer = new char[32 * 1024];
      while (resultSeq.hasNext()) {
        Reader buf = resultSeq.next().asReader();
        while (true) {
          int actual = buf.read(readBuffer);
          if (actual <= 0)
            break;
          resultsBuffer.append(readBuffer, 0, actual);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        resultSeq.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return resultsBuffer.toString().trim();
  }

  private void runQuery(XCCInfo xccInfo) throws Exception {
    ContentSource contentSource = null;
    contentSource = ContentSourceFactory.newContentSource(xccInfo.getHost(),
        xccInfo.getPort(), xccInfo.getUser(), xccInfo.getPassword());
    session = contentSource.newSession();
    AdhocQuery request = session.newAdhocQuery(xqStr);
    if (tStamp != null) {
      RequestOptions options = new RequestOptions();
      options.setEffectivePointInTime(new BigInteger(tStamp));
      request.setOptions(options);
    }
    ResultSequence rs = session.submitRequest(request);
    String value = getValueFromResults(rs);

    if (!value.equals(expectedValue)) {
      // log bad stuff
      System.out.println("*************************");
      System.out.println("****ERROR****************");
      System.out.println("*Ran query " + xqStr);
      System.out.println("*At time stamp " + tStamp);
      System.out.println("*Expected value " + expectedValue + " actual value "
          + value);
      System.out.println("*Run against xcc server " + xccInfo.toString());
      System.out.println("*************************");
      if (vData != null) {
        ReplicaValidationNotifier notifier = vData.getValidationNotifier();
        if (notifier != null)
          notifier.notifyReplicaValidationComplete(false);
      }
    }
    else
    {
      if (vData != null) {
        ReplicaValidationNotifier notifier = vData.getValidationNotifier();
        if (notifier != null)
          notifier.notifyReplicaValidationComplete(true);
      }
    }

    session.close();
    contentSource = null;
    session = null;
  }

  private void runValidationQuery() throws Exception {
    if (vData != null) {
      ReplicaValidationNotifier notifier = vData.getValidationNotifier();
      if (notifier != null)
        notifier.startProcessing();
    }
    // for each replica of the server
    for (int i = 0; i < connectionData.servers.get(serverIndex).replicas.size(); i++) {
      runQuery(connectionData.servers.get(serverIndex).replicas.get(i));
    }
    // ran query against all replicas so must be finished
    finished = true;
  }

  public void run() {
    while (!finished && numTries < 10) {
      try {
        runValidationQuery();
      } catch (Exception e) {
        e.printStackTrace();
      }
      if (!finished)
        numTries++;
    }
    // in case it exits with numTries eq 10 set finished to true
    finished = true;
    if (numTries == 10) {
      System.out.println("*************************");
      System.out.println("****ERROR****************");
      System.out.println("* could not successfully run query " + xqStr);
      System.out.println("* in " + numTries + " attempts");
      System.out.println("* at time stamp " + tStamp);
      System.out.println("* Relplicas :");
      for (int i = 0; i < connectionData.servers.get(serverIndex).replicas
          .size(); i++) {
        System.out.println("\t"
            + connectionData.servers.get(serverIndex).replicas.get(i)
                .toString());
      }
      System.out.println("*************************");

      if (vData != null) {
        ReplicaValidationNotifier notifier = vData.getValidationNotifier();
        if (notifier != null)
          notifier.notifyReplicaValidationComplete(false);
      }
    }
  }

}
