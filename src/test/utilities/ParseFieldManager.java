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

import java.io.PrintStream;
import java.util.Random;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * access to global information for the template parser
 */
public interface ParseFieldManager {

  /**
   * allow for a common random number generator
   */
  public Random getRandomNumberGenerator();

  /**
   * initialize the field manager from the config file
   */
  public void   initialize(Node node);

  /**
   * add a custom field that is not in the config file but allows
   * more control over the parse field than would be driven purely
   * from configuration data (e.g. a custom generated ID that is
   * used repeatedly throughout the template
   */
  public void addCustomField(String token, ParseField field);

  /**
   * get the parse field for a tag
   */
  public ParseField getParseField(String tag);

  /**
   * For debugging purposes:  Dump all the fields in the manager
   */
  public void dumpFieldList(PrintStream ps);
}

