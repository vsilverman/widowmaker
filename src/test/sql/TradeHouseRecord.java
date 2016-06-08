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

import org.joda.time.DateTime;

// a class to glue the work between two modules
// this allows us to keep the triples logic separate from
// the SQL driving

class TradeHouseRecord {
    String sub;
    int tradeHouseId;
    String name;
    String classId;

    TradeHouseRecord(String sub, int tradeHouseId) {
      this.sub = sub;
      this.tradeHouseId = tradeHouseId;
    }

    TradeHouseRecord(String sub, int tradeHouseId, String classId) {
      this.sub = sub;
      this.tradeHouseId = tradeHouseId;
      this.classId = classId;
    }

    TradeHouseRecord(String sub, int tradeHouseId, String classId, String name) {
      this.sub = sub;
      this.tradeHouseId = tradeHouseId;
      this.classId = classId;
      this.name = name;
    }

    TradeHouseRecord() {
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setTradeHouseId(int id) {
      this.tradeHouseId = id;
    }

    public int getTradeHouseId() {
      return tradeHouseId;
    }

    public void setClassId(String id) {
      this.classId = id;
    }

    public String getClassId() {
      return classId;
    }

    public Object clone() {
      TradeHouseRecord o = new TradeHouseRecord();

      o.sub = this.sub;
      o.tradeHouseId = this.tradeHouseId;
      o.classId = this.classId;
      o.name = this.name;

      return o;
    }

    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append(sub);
      sb.append(" ");
      sb.append(tradeHouseId);
      sb.append(" ");
      sb.append(classId);
      sb.append(" ");
      sb.append(name);

      return sb.toString();
    }

  }

