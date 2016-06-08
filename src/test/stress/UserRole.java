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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

/**
 * Representation of the server side role for a given user
 * The permission information is optional
 */
public class UserRole {

  protected String roleName;
  protected String rolePermission;

  public UserRole() {
  }

  public UserRole(String roleName) {
  }

  public UserRole(String roleName, String rolePermission) {
  }

  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public String getRolePermission() {
    return rolePermission;
  }

  public void setRolePermission(String rolePermission) {
    this.rolePermission = rolePermission;
  }

  public void init(Element element) {

    if (element == null)
      return;

    NodeList nameNodes = element.getElementsByTagName("name");
    NodeList permNodes = element.getElementsByTagName("permission");

    // do we allow this?
    if ((nameNodes == null) || (nameNodes.getLength() == 0)) {
      System.out.println("user role has empty name");
    } else {
      this.roleName = ((Element)nameNodes.item(0)).getTextContent();
    }
    if ((permNodes == null) || (permNodes.getLength() == 0)) {
      System.out.println("user role has empty perm");
    } else {
      this.rolePermission = ((Element)permNodes.item(0)).getTextContent();
    }
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();

    sb.append(roleName);
    sb.append("\n");
    if (rolePermission != null) {
      sb.append(rolePermission);
      sb.append("\n");
    }

    return sb.toString();
  }

}

