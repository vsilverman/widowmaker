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


package test.sql;

import java.util.Iterator;

import test.stress.ScheduleTestData;
import test.stress.QueryTestData;
import test.stress.Query;

public class SqlMonitorTestData
        extends ScheduleTestData {

  // embedding a QueryTestData because it's the easiest way to get
  // the query functionality without having to clone the parsing code
  protected QueryTestData queryTestData;

  public SqlMonitorTestData() {

  }

  public SqlMonitorTestData(String configFile)
      throws Exception {

    super(configFile);

    System.out.println("SqlMonitorTestData for file " + configFile);

    queryTestData = new QueryTestData(configFile);

  }

  public int getQueryCount() {
    if (queryTestData == null)
      return 0;

    return queryTestData.getQueries().size();
  }

  public Query getQueryAt(int pos) {
    if (queryTestData == null)
      return null;

    if (pos > getQueryCount())
      return null;

    Query query = queryTestData.getQueries().get(pos);

    return query;
  }

  public Iterator getQueries() {

    if (queryTestData == null)
      return null;

    return queryTestData.getQueries().iterator();

  }


}

