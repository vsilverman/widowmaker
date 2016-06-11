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

import java.util.ArrayList;
import java.util.List;

import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.types.XdmItem;

public class HashLockTester extends XccStressTester {
  private HashLockTestData hashLockTestData = null;
  private int numURIs = 1000;
  private int numThreads = 100;
  private String collectionURI = null;
  private List<String> uriList = new ArrayList<String>();
  private List<HashLockThread> threadList = new ArrayList<HashLockThread>();
  private String uriDelta = "";

  private class HashLockThread extends Thread {
    private String fileName = "hashlocktest.xml";
    private List<SessionHolder> threadSessions = null;

    public HashLockThread(List<SessionHolder> threadSessions) {
      this.threadSessions = threadSessions;
    }

    public void run() {
      for (int i = 0; i < threadSessions.size(); ++i)
        threadSessions.get(i).connect();

      for (int i = 0; i < numURIs; ++i) {
        String curURI = uriList.get(i) + uriDelta + fileName;
        String xqStr = "if (fn:not(fn:doc-available('" + curURI + "'))) "
            + "then xdmp:document-insert('" + curURI
            + "', <number>1</number>, " + "(), '" + collectionURI + "') "
            + "else " + "let $doc := fn:doc('" + curURI + "') "
            + "let $count := fn:data($doc/number) "
            + "let $newCount := $count + 1 "
            + "return xdmp:node-replace($doc/number, "
            + "<number>{$newCount}</number>)";

        for (int j = 0; j < threadSessions.size(); ++j)
          threadSessions.get(j).runQuery(xqStr);
      }

      for (int i = 0; i < threadSessions.size(); ++i)
        threadSessions.get(i).disconnect();
    }
  }

  public HashLockTester(ConnectionData connData, HashLockTestData hashLockData,
      String threadName) {
    super(connData, hashLockData, threadName);
    hashLockTestData = hashLockData;
  }

  private String generateUniqueURI() {
    return new String("/" + randomString(16) + "/" + getDateString() + "/"
        + threadName + "/");
  }

  // METHOD runTest() a pure virtual method, sub classes must implement
  public void runTest() {
    try {
      System.out.println("Starting test ");
      System.out.println(hashLockTestData.toString());
      for (int i = 0; i < hashLockTestData.getNumOfLoops() && alive; i++) {
        numURIs = hashLockTestData.getNumURIs();
        collectionURI = generateUniqueURI();
        for (int j = 0; j < numURIs; ++j)
          uriList.add(j, generateUniqueURI());

        numThreads = hashLockTestData.getNumThreads();
        for (int j = 0; j < numThreads; ++j) {
          List<SessionHolder> threadSessions = new ArrayList<SessionHolder>();
          for (SessionHolder s : sessions)
            threadSessions.add(new SessionHolder(s.contentSource));
          threadList.add(j, new HashLockThread(threadSessions));
        }

        // insert and update docs
        for (int j = 0; j < numThreads; ++j)
          threadList.get(j).start();
        for (int j = 0; j < numThreads; ++j)
          threadList.get(j).join();

        // verify all docs have the correct sum
        for (SessionHolder s : sessions) {
          String xqStr = "fn:data(fn:collection('" + collectionURI
              + "')/number)";
          s.connect();
          ResultSequence rs = s.runQuery(xqStr, null);
          s.disconnect();

          int counter = 0;
          while (rs.hasNext()) {
            counter++;
            ResultItem rsItem = rs.next();
            XdmItem item = rsItem.getItem();
            int result = Integer.parseInt(item.asString());
            if (result != numThreads)
              System.out.println("ERROR Expected the number: " + numThreads
                  + ", in document but instead got: " + result);
          }
          if (counter != numURIs)
            System.out.println("ERROR Expected: " + numThreads
                + ", URIs but instead got: " + counter);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
