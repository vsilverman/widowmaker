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

class CustomerRecord {
    String sub;
    int custId;
    String classId;
    int traderId;
    String name;
    DateTime lastDate;

    CustomerRecord(String sub, int custId) {
      this.sub = sub;
      this.custId = custId;
    }

    CustomerRecord(String sub, int custId, String classId) {
      this.sub = sub;
      this.custId = custId;
      this.classId = classId;
    }

    CustomerRecord(String sub, int custId, String classId, int traderId) {
      this.sub = sub;
      this.custId = custId;
      this.classId = classId;
      this.traderId = traderId;
    }

    CustomerRecord(String sub, int custId, String classId,
                      int traderId, DateTime lastDate) {
      this.sub = sub;
      this.custId = custId;
      this.classId = classId;
      this.traderId = traderId;
      this.lastDate = lastDate;
    }

    CustomerRecord() {
    }

    public void setLastDate(String str) {
      lastDate = new DateTime(str);
    }

    public void setLastDate(DateTime lastDate) {
      this.lastDate = lastDate;
    }

    public void setCustId(int id) {
      this.custId = id;
    }

    public int getCustId() {
      return custId;
    }

    public int getCustomerId() {
      return custId;
    }

    public String getClassId() {
      return classId;
    }

    public void setClassId(String id) {
      this.classId = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Object clone() {
      CustomerRecord o = new CustomerRecord();

      o.sub = this.sub;
      o.custId = this.custId;
      o.classId = this.classId;
      o.traderId = this.traderId;
      o.name = this.name;
      o.lastDate = this.lastDate;

      return o;
    }

    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append(sub);
      sb.append(" ");
      sb.append(custId);
      sb.append(" ");
      sb.append(classId);
      sb.append(" ");
      sb.append(traderId);
      sb.append(" ");
      sb.append(name);
      if (lastDate != null) {
        sb.append(" ");
        sb.append(lastDate.toString());
      }

      return sb.toString();
    }

  }

