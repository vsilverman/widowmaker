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

import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Hashtable;
import java.util.Collection;
import java.util.Random;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * a singleton manager to manage ConnectionData both as a global
 * service as well as provide for multiple connections for various
 * identities
 */
public class ConnectionDataManager {

  private static ConnectionDataManager thisConnectionDataManager = new ConnectionDataManager();
  private ConnectionData connectionData = null;
  private Random randNum;
  
  private ConnectionDataManager() {

    randNum = new Random();
  }

  public static ConnectionDataManager getConnectionDataManager() {
    return thisConnectionDataManager;
  }

  public void initialize(String filename) {

    if (filename == null) {
      System.err.println("getConnectionData:  filename is null");
      return ;
    }

    File file = new File(filename);
    if (!file.exists()) {
      System.err.println("getConnectionData:  filename doesn't exist - " + filename);
      return ;
    }

    System.out.println("initializing connection data:  " + filename);

    try {
      connectionData = new ConnectionData(filename);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public String toString() {

    if (getConnectionData() != null)
      return getConnectionData().toString();

    return null;
  }


  /**
   * The expected primary method used by most extensions. This returns the
   * primary connection within the system if there is only one connection
   * configuration, or returns a random connection configuration if there are
   * more than one present.
   */
  public static ConnectionData getConnectionData() {

    ConnectionDataManager mgr = getConnectionDataManager();

    return mgr.connectionData;
  }


  /**
   * Used to retrieve the connection data while initializing it from a config file
   */
  public static ConnectionData getConnectionData(String filename)
      throws Exception {

    ConnectionDataManager mgr = getConnectionDataManager();
    mgr.initialize(filename);

    return mgr.connectionData;
  }

  /**
   * Anticipating:  get a random connection for a given user
   */

  private static void usage() {
    System.out.println("usage ConnectionDataManager configfile.xml");
  }

  public static void main(String[] args) {

    if (args.length == 0) {
      usage();
      System.exit(1);
    }

    ConnectionDataManager mgr = getConnectionDataManager();
    try {
      mgr.initialize(args[0]);
      System.out.println(mgr.toString());
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }

    System.exit(0);
  }
}

