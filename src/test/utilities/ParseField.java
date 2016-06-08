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


package test.utilities;

import java.util.HashMap;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.w3c.dom.Node;


/**
 * a field type that has a token and can generate data for a given
 * element or attribute within a parse template. The goal is to have
 * the template be format-agnostic: the field is configured from a
 * configuration XML file, and when invoked is a string in (token)
 * yielding a string out (value) - this should ideally work whether the
 * resulting data is XML, Javascript, or text. Time will tell if this works.
 */

public interface ParseField {

  public void initialize(ParseFieldManager manager, Node configNode);

  /**
   * handle to fetch the token after the field is initialized
   * used by the field manager to do dispatching
   */
  public String getToken();

  /**
   * produce data for the supplied token
   */
  public String generateData(String token);
  
  /**
   * produce data for the supplied token, using the supplied context data to help
   * shape the result.
   *
   * TODO:  should this be of type Object instead of String in order to provide
   * more flexibility?
   */
  public String generateData(String token, HashMap contextData);
  
}

