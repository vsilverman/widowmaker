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

/************************************************************************************************
 * XCCInfo class holds all information about what actions are to be performed by XccStressTester
 *
 *************************************************************************************************/
package test.stress;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

/**
 * This is a representation of the information needed to make an XCC connection
 */
public class XCCInfo {
  // all connection information should be in XCCInfo
  private String xccHost;
  private String xccUser;
  private String xccPass;
  private int xccLag = Integer.MAX_VALUE;
  private int xccPort;
  private String xccDatabase;

  public XCCInfo(String host, int port, String user, String password) {
    xccHost = host;
    xccPort = port;
    xccUser = user;
    xccPass = password;
  }

  public XCCInfo(String host, int port, String user, String password, String database) {
    xccHost = host;
    xccPort = port;
    xccUser = user;
    xccPass = password;
    xccDatabase = database;
  }

  /**
   * XCCInfo constructor comment.
   */
  public XCCInfo(Element serverNode) {
    Node n;

    n = serverNode.getElementsByTagName("host").item(0);
    if (n != null)
      xccHost = n.getTextContent();
    n = serverNode.getElementsByTagName("username").item(0);
    if (n != null)
      xccUser = n.getTextContent();
    n = serverNode.getElementsByTagName("password").item(0);
    if (n != null)
      xccPass = n.getTextContent();
    n = serverNode.getElementsByTagName("port").item(0);
    if (n != null) {
      if (n.getTextContent().length() > 0)
        xccPort = Integer.parseInt(n.getTextContent());
    }
    n = serverNode.getElementsByTagName("database").item(0);
    if (n != null)
      xccDatabase = n.getTextContent();

/*
    xccHost = serverNode.getElementsByTagName("host").item(0).getTextContent();
    xccUser = serverNode.getElementsByTagName("username").item(0)
        .getTextContent();
    xccPass = serverNode.getElementsByTagName("password").item(0)
        .getTextContent();
    xccPort = Integer.parseInt(serverNode.getElementsByTagName("port").item(0)
        .getTextContent());
    xccDatabase = serverNode.getElementsByTagName("database").item(0)
        .getTextContent();
*/
    // optional lag element
    n = serverNode.getElementsByTagName("lag").item(0);
    if (n != null) {
      String lag = serverNode.getElementsByTagName("lag").item(0).getTextContent();
      // optional element
      if (lag.length() > 0)
        xccLag = Integer.parseInt(lag);
    }
  }

  public String getHost() {
    return xccHost;
  }

  public String getUser() {
    return xccUser;
  }

  public String getPassword() {
    return xccPass;
  }

  public int getPort() {
    return xccPort;
  }

  public String getDatabase() {
    return xccDatabase;
  }

  public int getLag() {
    return xccLag;
  }

  public boolean equals(XCCInfo rh) {

    if (rh == null)
      return false;
    if (!this.xccHost.equalsIgnoreCase(rh.xccHost))
      return false;
    if (this.xccPort != rh.xccPort)
      return false;
    if (!this.xccUser.equalsIgnoreCase(rh.xccUser))
      return false;
    if (!this.xccPass.equalsIgnoreCase(rh.xccPass))
      return false;
    if (this.xccLag != rh.xccLag)
      return false;
    if ((this.xccDatabase == null) && (rh.xccDatabase != null))
      return false;
    if ((this.xccDatabase != null) && (rh.xccDatabase == null))
      return false;
    if (!this.xccDatabase.equalsIgnoreCase(rh.xccDatabase))
      return false;

    return true;
  }

  public String toString() {
    String temp = "\n";
    temp += "Host = " + xccHost + "\n";
    temp += "User = " + xccUser + "\n";
    temp += "Password = " + xccPass + "\n";
    temp += "Port = " + xccPort + "\n";
    temp += "Database = " + xccDatabase + "\n";
	if (xccLag != Integer.MAX_VALUE)
		temp += "Lag = " + xccLag + "\n";
    return temp;
  }

}
