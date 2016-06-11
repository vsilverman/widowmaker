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

/************************************************************************************************
 * ConnectionData class holds all information about what actions are to be performed by XccStressTester
 *
 *************************************************************************************************/
package test.stress;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;

import javax.transaction.xa.XAResource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

import com.arjuna.ats.arjuna.recovery.RecoveryManager;
import com.arjuna.ats.arjuna.recovery.RecoveryModule;
import com.arjuna.ats.internal.jta.recovery.arjunacore.XARecoveryModule;
import com.arjuna.ats.jta.recovery.XAResourceRecoveryHelper;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;

/**
 * Insert the type's description here.
 */
public class ConnectionData {
  private static boolean DEBUG_FLAG = false;
  private Random randNum;

/*
  private class User {
    String username;
    String password;

    User(String username, String password) {
      this.username = username;
      this.password = password;
    }
  }

  private ArrayList<User> users = new ArrayList<User>();
*/
  private ArrayList<ConnectionUser> users = new ArrayList<ConnectionUser>();

  private int lastUserDistributed = -1;

  public static class Server {
    public XCCInfo info;
    public ContentSource contentSource = null;
    public List<XCCInfo> replicas = new ArrayList<XCCInfo>();

    private void debug() {
      if (DEBUG_FLAG) {
        System.out.println("creating new Server record for:");
        if (info == null)
          System.out.println("XCCInfo is null!!!!!!");
        System.out.println(info.toString());
      }
    }

    Server(Element serverNode) {
      info = new XCCInfo(serverNode);

      if (DEBUG_FLAG)
        System.out.println("processing replicas");

      NodeList replicaNodes = serverNode.getElementsByTagName("replica");
      addReplicas(replicaNodes);

      debug();

      contentSource = ContentSourceFactory.newContentSource(info.getHost(),
          info.getPort(), info.getUser(), info.getPassword());
    }

    void addReplicas(NodeList replicaNodes) {

      for (int i = 0; i < replicaNodes.getLength(); ++i) {
        Element replicaNode = (Element) replicaNodes.item(i);
        XCCInfo repInfo = new XCCInfo(replicaNode);
        if (DEBUG_FLAG) {
          System.out.println("adding replica " + i + ":");
          System.out.println(repInfo.toString());
        }
        replicas.add(repInfo);
      }

    }

    Server(String host, int port, String user, String password) {
      info = new XCCInfo(host, port, user, password);

      debug();

      contentSource = ContentSourceFactory.newContentSource(info.getHost(),
          info.getPort(), info.getUser(), info.getPassword());
    }
    Server(String host, int port, String user, String password, String database) {
      info = new XCCInfo(host, port, user, password, database);

      debug();

      contentSource = ContentSourceFactory.newContentSource(info.getHost(),
          info.getPort(), info.getUser(), info.getPassword());
    }

    // TODO:  is this okay, or do we need to clone? Can we trust them?
    public XCCInfo getInfo() {
      return info;
    }

    // we need to expose the content source for those not in our package
    public ContentSource getContentSource() {
      return contentSource;
    }
  }

  public final List<Server> servers = new ArrayList<Server>();

  public ConnectionData(XCCInfo info) throws Exception {
    randNum = new Random();
    Server server = new Server(info.getHost(), info.getPort(), info.getUser(), info.getPassword());
    servers.add(server);
  }

  /**
   * Insert the method's description here.
   * 
   * @param fileName
   *          java.lang.String
   */
  public ConnectionData(String fileName) throws Exception {
      // load the file into a DOM object.
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document connectDocument = builder.parse(new File(fileName));
      NodeList serverNodes = connectDocument.getDocumentElement()
              .getElementsByTagName("server");
      if (DEBUG_FLAG) {
        System.out.println("total server nodes:  " + serverNodes.getLength());
      }

      for (int i = 0; i < serverNodes.getLength(); ++i) {
        Element serverNode = (Element) serverNodes.item(i);

        if (DEBUG_FLAG)
          System.out.println("processing server node " + i);

        NodeList replicaNodes = serverNode.getElementsByTagName("replica");

        // need to look through here and decide what we have
        // if we have only one user/pw pair, we can just populate this
        // node and go forward; if, however, we have a users element,
        // we then have a series of user/pw pairs, and we need a server
        // instance for each of them

        NodeList usersNode = serverNode.getElementsByTagName("users");
        if ((usersNode == null) || (usersNode.getLength() == 0)) {
          if (DEBUG_FLAG) {
            System.out.println("users is null, creating individual server node");
          }
          servers.add(new Server(serverNode));
        } else {
          createUserList(usersNode);
          if (DEBUG_FLAG) {
            System.out.println("users is full, creating multiple server nodes");
            System.out.println("usersNode length is " + usersNode.getLength());
          }
          Element usersElement = (Element)usersNode.item(0);
          NodeList hostList = serverNode.getElementsByTagName("host");
          NodeList portList = serverNode.getElementsByTagName("port");
          NodeList lagList = serverNode.getElementsByTagName("lag");
          // database doesn't have to be present
          NodeList dbList = serverNode.getElementsByTagName("database");
          Element dbElement = (Element)dbList.item(0);
          if ((hostList == null) || (portList == null))
            throw new Exception("Invalid connection info in " + fileName);
          Element hostElement = (Element)hostList.item(0);
          Element portElement = (Element)portList.item(0);
          Node n;
          String host = null;
          String database = null;
          int port;
          int lag = Integer.MAX_VALUE;
          if (hostElement == null)
            throw new Exception("Invalid connection info:  missing host in " + fileName);
          host = hostElement.getTextContent();
          if (DEBUG_FLAG) {
            System.out.println("host is " + host);
          }
          if (portElement == null)
            throw new Exception("Invalid connection info:  missing port in " + fileName);
          port = Integer.parseInt(portElement.getTextContent());
          if (DEBUG_FLAG) {
            System.out.println("port is " + port);
          }
          if (dbElement != null) {
            database = dbElement.getTextContent();
          }
          if (DEBUG_FLAG) {
            System.out.println("database is " + database);
          }
          if ((lagList != null) && (lagList.getLength() > 0)) {
            Element lagElement = (Element)lagList.item(0);
            lag = Integer.parseInt(lagElement.getTextContent());
          }

          NodeList userNodes = usersElement.getElementsByTagName("user");
          for (int jj = 0; jj < userNodes.getLength(); jj++) {
            if (DEBUG_FLAG) {
              System.out.println("preparing connection info for user " + jj);
            }
            Element userNode = (Element)userNodes.item(jj);
            NodeList usernameList = userNode.getElementsByTagName("username");
            NodeList passwordList = userNode.getElementsByTagName("password");
            if ((usernameList == null) || (passwordList == null)) {
              throw new Exception("Invalid connection info:  missing user detail in position " + jj);
            }
            Element usernameElement = (Element)usernameList.item(0);
            Element passwordElement = (Element)passwordList.item(0);
            String username = usernameElement.getTextContent();
            String password = passwordElement.getTextContent();
            if (DEBUG_FLAG) {
              System.out.println("username is " + username);
              System.out.println("password is " + password);
            }
            Server server = new Server(host, port, username, password, database);
            server.addReplicas(replicaNodes);
            servers.add(server);
          }
        }
      }

      if (servers.size() > 1) {
          // JBoss specific recovery setup
          RecoveryManager rm = RecoveryManager.manager();
          for (RecoveryModule mod : rm.getModules()) {
              if (mod instanceof XARecoveryModule) {
                  ((XARecoveryModule) mod)
                          .addXAResourceRecoveryHelper(new XAResourceRecoveryHelper() {
                              public boolean initialise(String p) throws Exception {
                                  return true;
                              }

                              public XAResource[] getXAResources() throws Exception {
                                  List<XAResource> result = new ArrayList<XAResource>();
                                  for (Server s : servers)
                                      result.add(s.contentSource.newSession().getXAResource());
                                  return result.toArray(new XAResource[0]);
                              }
                          });
                  break;
              }
          }
      }
  }

  private void createUserList(NodeList usersList) 
      throws Exception {

    Element usersElement = (Element)usersList.item(0);
    Node n;
    NodeList userNodes = usersElement.getElementsByTagName("user");
    for (int jj = 0; jj < userNodes.getLength(); jj++) {
      if (DEBUG_FLAG) {
        System.out.println("preparing user info for user " + jj);
      }
      Element userNode = (Element)userNodes.item(jj);



/*
      NodeList usernameList = userNode.getElementsByTagName("username");
      NodeList passwordList = userNode.getElementsByTagName("password");
      if ((usernameList == null) || (passwordList == null)) {
        throw new Exception("Invalid user info:  missing user detail in position " + jj);
      }
      Element usernameElement = (Element)usernameList.item(0);
      Element passwordElement = (Element)passwordList.item(0);
      String username = usernameElement.getTextContent();
      String password = passwordElement.getTextContent();
      if (DEBUG_FLAG) {
        System.out.println("username is " + username);
        System.out.println("password is " + password);
      }
      User user = new User(username, password);
      users.add(user);
*/
      ConnectionUser user = new ConnectionUser();
      user.init(userNode);
      users.add(user);


    }
  }

  public String getRandomUser() {
    if (users.size() == 0)
      return null;
    else if (users.size() == 1)
      return ((ConnectionUser)users.get(0)).username;
    else {
      int index = Math.abs(randNum.nextInt() % users.size());
      return ((ConnectionUser)users.get(index)).username;
    }
  }

  /**
   * this is synchronized to make sure only one thread is advancing the
   * next user reference at a time
   */
  public synchronized String getNextUser() {

    if (++lastUserDistributed > users.size())
      lastUserDistributed = 0;
    ConnectionUser user = (ConnectionUser)users.get(lastUserDistributed);
    if (user != null) {
      if (DEBUG_FLAG)
        System.out.println("returning index, user "
                    + lastUserDistributed + ", " + user.username);
      return user.username;
    } else {
        System.out.println("returning index, user "
                    + lastUserDistributed + ", null ");
      return null;
    }
  }

  public String getPasswordForUser(String username) {
    ConnectionUser user = null;
    for (int ii = 0; ii < users.size(); ii++) {
      user = (ConnectionUser)users.get(ii);
      if (user.username.equalsIgnoreCase(username))
        return user.password;
    }
    return null;
  }

  public ContentSource getServer(XCCInfo info) {

    if (info == null)
      return null;

    Iterator iter = servers.iterator();
    while (iter.hasNext()) {
      Server server = (Server)iter.next();
      if (server.info.equals(info))
        return server.contentSource;
    }

    return null;
  }

  /**
   * most configurations will have one server configured, and we'll
   * just hand back the first one. If serverName is null, we'll do that
   * as well; otherwise, we'll find the server name and pass back the info
   */
  public XCCInfo getServerInfo(String serverName) {

    if (servers == null)
      return null;

    Iterator iter = servers.iterator();
    while (iter.hasNext()) {
      Server server = (Server)iter.next();
      if (serverName == null)
        return server.getInfo();
      if (server.getInfo().getHost().equals(serverName))
        return server.getInfo();
    }

    return null;
  }

  public ContentSource getContentSourceForUser(String user) {
    int size = servers.size();
    int count = 0;
    Server[] datas = new Server[size];
    for (int ii = 0; ii < size; ii++) {
      Server server = (Server)servers.get(ii);
      if (server.info.getUser().equals(user)) {
        datas[count++] = server;
      }
    }

    ContentSource source = null;
    if (count == 0)
      source = null;
    else if (count == 1)
      source = datas[0].contentSource;
    else {
      int index = Math.abs(randNum.nextInt() % count);
      source =  datas[index].contentSource;
    }

    return source;
  }

  public String toString() {

    StringBuffer sb = new StringBuffer();

    Iterator iter = users.iterator();
    while (iter.hasNext()) {
      ConnectionUser user = (ConnectionUser)iter.next();
      sb.append(user.toString());
    }

    return sb.toString();
  }


  /*
   * public String toString() { String temp = "\n"; temp += "ServerName = " +
   * fieldServerName + "\n"; temp += "UserID = " + fieldUserID + "\n"; temp +=
   * "Password = " + fieldPassword + "\n"; temp += "ConnectString = " +
   * fieldConnectString + "\n"; return temp; }
   */

}
