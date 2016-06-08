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

import java.io.Reader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import javax.transaction.TransactionManager;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;

public abstract class XccStressTester extends StressTester {
  protected ConnectionData connData = null;
  private TransactionManager tm = com.arjuna.ats.jta.TransactionManager
      .transactionManager();

  protected static class SessionHolder {
    public ContentSource contentSource;
    public Session session = null;

    SessionHolder(ContentSource cs) {
      this.contentSource = cs;
    }

    public void connect() {
      session = contentSource.newSession();
    }

    public void disconnect() {
      if (session != null) {
        session.close();
        session = null;
      }
    }

    public ResultSequence runQuery(Query query, RequestOptions options)
        throws RequestException {
      AdhocQuery request = session.newAdhocQuery(query.query);
      if (options != null) {
        options.setQueryLanguage(query.language);
      } else {
        options = new RequestOptions();
        options.setQueryLanguage(query.language);
      }
      request.setOptions(options);
        
      return session.submitRequest(request);
    }

    public ResultSequence runQuery(String query, RequestOptions options)
        throws RequestException {
      AdhocQuery request = session.newAdhocQuery(query);
      if (options != null) {
        request.setOptions(options);
      } 
      return session.submitRequest(request);
    }

    public void runQuery(Query query) {
      try {
        AdhocQuery request = session.newAdhocQuery(query.query);
        RequestOptions options = new RequestOptions();
        options.setQueryLanguage(query.language);
        request.setOptions(options);
 
        session.submitRequest(request).close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
 
    public void runQuery(String xqStr) {
      try {
        session.submitRequest(session.newAdhocQuery(xqStr)).close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  // note session index is the same as the server index
  // please do not change that the replica validation relies on this fact
  protected List<SessionHolder> sessions = new ArrayList<SessionHolder>();

  public XccStressTester(ConnectionData connData, TestData testData,
                         String threadName) {
    initialize(connData, testData, threadName);
  }

  public XccStressTester(TestData testData,
                         String threadName) {
    ConnectionData connData = ConnectionDataManager.getConnectionData();
    initialize(connData, testData, threadName);
  }

  public XccStressTester() {
  }

  public void initialize(ConnectionData connData, TestData testData,
                         String threadName) {
    this.connData = connData;
    this.testData = testData;
    this.threadName = threadName;
    isPerfTest = testData.getIsPerfTest();
    alive = true;
    // note session index is the same as the server index in connection data
    // class
    // please do not change that the replica validation relies on this fact
    for (ConnectionData.Server s : connData.servers) {
      sessions.add(new SessionHolder(s.contentSource));
    }
  }

  public void initialize(TestData testData, String threadName) {
    this.connData = ConnectionDataManager.getConnectionData();
    this.testData = testData;
    this.threadName = threadName;
    isPerfTest = testData.getIsPerfTest();
    alive = true;
    // note session index is the same as the server index in connection data
    // class
    // please do not change that the replica validation relies on this fact
    for (ConnectionData.Server s : connData.servers) {
      sessions.add(new SessionHolder(s.contentSource));
    }
  }

  public abstract void runTest();

  public void connect() throws Exception {
    for (SessionHolder s : sessions)
      s.connect();
  }

  public void disconnect() {
    for (SessionHolder s : sessions)
      s.disconnect();
  }

  public void beginTransaction() throws Exception {
    if (sessions.size() > 1) {
      tm.begin();
      for (SessionHolder s : sessions)
        tm.getTransaction().enlistResource(s.session.getXAResource());
    } else if (testData.getMultiStatement()) {
      try {
        sessions.get(0).session.rollback();
      } catch (Exception e) {
      }
      long startTime = System.currentTimeMillis();
      sessions.get(0).session
          .setTransactionMode(Session.TransactionMode.UPDATE);
      totalTime += System.currentTimeMillis() - startTime;
    }
  }

  public void commitTransaction() throws Exception {
    if (sessions.size() > 1) {
      tm.commit();
    } else if (testData.getMultiStatement()) {
      long startTime = System.currentTimeMillis();
      sessions.get(0).session.commit();
      totalTime += System.currentTimeMillis() - startTime;
      sessions.get(0).session.setTransactionMode(Session.TransactionMode.AUTO);
      if (isPerfTest) {
        accumulateResponse();
        if (responseTimes.size() >= 100 || testData.getBatchSize() >= 100) {
          addOrUpdateResponseDoc();
        }
      }
    }
    totalTime = 0;
  }

  public void addOrUpdateResponseDoc() {
      super.addOrUpdateResponseDoc();
      String timeDocURI = "time-doc.xml";
      String xqStr = "xquery version '1.0-ml';" + " if(exists(doc('" + timeDocURI
              + "')))" + "   then  xdmp:node-insert-child( doc('" + timeDocURI
              + "')/* , " + getResponseElements(true) + ")"
              + "   else xdmp:document-insert( '" + timeDocURI + "', <times> "
              + getResponseElements(false) + "</times>)";
      for (SessionHolder s : sessions)
        s.runQuery(xqStr);
  }

  public void rollbackTransaction() throws Exception {
    if (sessions.size() > 1) {
      tm.rollback();
    } else if (testData.getMultiStatement()) {
      sessions.get(0).session.rollback();
      sessions.get(0).session.setTransactionMode(Session.TransactionMode.AUTO);
    }
  }

  public void setTransactionTimeout(int t) throws Exception {
    if (sessions.size() > 1) {
      tm.setTransactionTimeout(t);
    } else if (testData.getMultiStatement()) {
      sessions.get(0).session.setTransactionTimeout(t);
    }
  }
}
