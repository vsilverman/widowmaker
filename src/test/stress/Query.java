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

import java.util.HashMap;

public class Query {
  public String query;
  public String language = "xquery";
  public String phase = null;
  public HashMap<String, String> attributes = null;

  public Query(String query, String language) {
    this.query = query;
    if (language != null && !language.isEmpty()) {
        this.language = language;
    }
    attributes = new HashMap<String, String>();
  }
  
  public void setPhase(String phase) {
    this.phase = phase;
  }

  public String getPhase() {
    return phase;
  }

  public void addAttribute(String name, String value) {
    if (name == null || value == null)
      return;
    attributes.put(name, value);
  }

  public String getAttribute(String name) {
    if (name == null)
      return null;
    return attributes.get(name);
  }

  /**
   * Takes a string representation of a phase of the test for which the query might be run
   * and determines if the phase is found in the phase attribute of the query.
   * The phase attribute can contain one or more tokens, which need only be distinct enough
   * to locate with a contains() check.
   */
  public boolean isPhase(String chkPhase) {
    if ((chkPhase == null) || (phase == null))
      return false;
    String thisPhase = phase.toLowerCase();
    String cPhase = chkPhase.toLowerCase();
    return thisPhase.contains(cPhase);
  }

  @Override
  public String toString() {
    return query;
  }
}
