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
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

/**
 * Client side representation of user hanging off the ConnectionData
 * configuration.
 */
public class ConnectionUser {

  protected String username;
  protected String password;
  protected ArrayList<UserRole> roles = null;

  public ConnectionUser() {

    roles = new ArrayList<UserRole>();
  }

  public ConnectionUser(String name) {

    username = name;
    roles = new ArrayList<UserRole>();

  }

  public String getUserName() {
    return username;
  }

  public void setUserName(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void init(Element element) {

    if (element == null)
      return;

    NodeList userList = element.getElementsByTagName("username");
    NodeList passwordList = element.getElementsByTagName("password");
    NodeList rolesList = element.getElementsByTagName("roles");

    if ((userList == null) || (userList.getLength() == 0)) {
      System.out.println("user has no user element");
    } else {
      Element userElement = (Element)userList.item(0);
      username = userElement.getTextContent();
    }
    if ((passwordList == null) || (passwordList.getLength() == 0)) {
      System.out.println("user has no password element");
    } else {
      Element passwordElement = (Element)passwordList.item(0);
      password = passwordElement.getTextContent();
    }

    if ((rolesList == null) || (rolesList.getLength() == 0)) {
    } else {
      for (int ii = 0; ii < rolesList.getLength(); ii++) {
        Element rolesElement = (Element)rolesList.item(ii);
        NodeList roleList = rolesElement.getElementsByTagName("role");
        if ((roleList != null) && (roleList.getLength() > 0)) {

          for (int jj = 0; jj < roleList.getLength(); jj++ ) {
            Element roleNode = (Element)roleList.item(jj);
            UserRole role = new UserRole();
            role.init(roleNode);
            roles.add(role);
          }
        }


      }
    }
  }

  public String toString() {

    StringBuffer sb = new StringBuffer();

    sb.append(username);
    sb.append("\n");
    sb.append(password);
    sb.append("\n");
    Iterator iter = roles.iterator();
    while (iter.hasNext()) {
      UserRole role = (UserRole)iter.next();
      sb.append(role.toString());
    }

    return sb.toString();
  }


}

