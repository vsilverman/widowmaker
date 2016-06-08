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

class TraderRecord {
    String sub;
    int traderId;
    String rating;
    int tradeHouse;
    String name;

    TraderRecord(String sub, int traderId) {
      this.sub = sub;
      this.traderId = traderId;
    }

    TraderRecord(String sub, int traderId, String rating) {
      this.sub = sub;
      this.traderId = traderId;
      this.rating = rating;
    }

    TraderRecord(String sub, int traderId, String rating, int tradeHouse) {
      this.sub = sub;
      this.traderId = traderId;
      this.rating = rating;
      this.tradeHouse = tradeHouse;
    }

    TraderRecord(String sub, int traderId, String rating,
                      int tradeHouse, String name) {
      this.sub = sub;
      this.traderId = traderId;
      this.rating = rating;
      this.tradeHouse = tradeHouse;
      this.name = name;
    }

    TraderRecord() {
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setTraderId(int id) {
      this.traderId = id;
    }

    public int getTraderId() {
      return traderId;
    }

    public String getRating() {
      return rating;
    }

    public void setRating(String rating) {
      this.rating = rating;
    }

    public Object clone() {
      TraderRecord o = new TraderRecord();

      o.sub = this.sub;
      o.traderId = this.traderId;
      o.rating = this.rating;
      o.tradeHouse = this.tradeHouse;
      o.name = this.name;

      return o;
    }

    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append(sub);
      sb.append(" ");
      sb.append(traderId);
      sb.append(" ");
      sb.append(rating);
      sb.append(" ");
      sb.append(tradeHouse);
      sb.append(" ");
      sb.append(name);

      return sb.toString();
    }

  }

