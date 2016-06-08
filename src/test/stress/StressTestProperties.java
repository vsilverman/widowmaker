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
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Enumeration;

/**
 * A run-wide notion of properties that are available for any module to fetch
 * configuration properties from. A singleton class, this is populated by
 * StressTest either by a cmdline switch to set, or by fetching as a resource
 * the default filename.
 */
public class StressTestProperties {

  private Properties properties = null;

  private static StressTestProperties stressProperties = new StressTestProperties();

  public static final String DEFAULT_PROPERTIES_FILE = "StressTestProperties.xml";

  private static final boolean DEBUG_FLAG = false;

  private StressTestProperties() {

    properties = new Properties();

  }

  /**
   * this just forces a restart with a set of defaults
   * Since this is private, we're expecting that the caller knows
   * that this is a total reset of what was already done.
   */
  private void createWithDefaults(Properties defaults) {
    if (DEBUG_FLAG)
      System.out.println("createWithDefaults");

    properties = new Properties(defaults);
  }

  public static final StressTestProperties getStressTestProperties() {
    return stressProperties;
  }

  private void loadPropertiesFromFile(String filename)
      throws IOException {

    if (filename == null)
      throw new FileNotFoundException("null properties file name");

    // first, let's see if this is an explicit file
    File f = new File(filename);
    if (f.exists()) {
      FileInputStream fis = new FileInputStream(f);
      loadPropertiesFromStream(fis);
    } else {
      InputStream is = StressTestProperties.class.getResourceAsStream(filename);
      if (is == null)
        throw new FileNotFoundException("cannot locate properties file:  " + filename);
      loadPropertiesFromStream(is);
    }

  }

  private void loadPropertiesFromStream(InputStream stream)
      throws IOException {

    getStressTestProperties().properties.loadFromXML(stream);
    stream.close();

  }

  public void initialize()
      throws IOException {

    try {
      initialize(DEFAULT_PROPERTIES_FILE, null);
    } catch (FileNotFoundException e) {
      // if we're working off the basic file and it's not there, don't worry
      System.out.println("Properties file not found:  " + DEFAULT_PROPERTIES_FILE);
    }
  }

  public void initialize(Properties defaults)
      throws IOException {

    try {
      initialize(DEFAULT_PROPERTIES_FILE, defaults);
    } catch (FileNotFoundException e) {
      // if we're working off the basic file and it's not there, don't worry
      System.out.println("Properties file not found:  " + DEFAULT_PROPERTIES_FILE);
    }
  }

  public void initialize(String filename)
      throws IOException {

    initialize(filename, null);

  }

  public void initialize(String filename, Properties defaults)
      throws IOException {

    System.out.println("StressTestProperties:  loading from " + filename);
    if (defaults != null)
      createWithDefaults(defaults);
    loadPropertiesFromFile(filename);

  }

  public String getProperty(String property) {
    if ((properties == null) || (property == null))
      return null;
    return properties.getProperty(property);
  }

  public void setProperty(String property, String value) {
    if ((properties == null) || (property == null) || (value == null))
      return ;
    properties.setProperty(property, value);
  }

  /**
   * fetches property and handles replacement of standard QA environment
   * variables performing substitution
   */
  public String getPropertyAsPath(String property) {

    if ((properties == null) || (property == null))
      return null;

    String prop = properties.getProperty(property);
    if (prop != null) {
      prop = prop.replaceAll("QA_HOME", System.getProperty("QA_HOME"));
    }

    return prop;
  }

  public Enumeration getPropertyNames() {
    if (properties == null)
      return null;

    return properties.propertyNames();
  }

}

