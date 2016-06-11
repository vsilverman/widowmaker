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


package test.sql;

import org.joda.time.DateTime;

// a class to glue the work between two modules
// this allows us to keep the triples logic separate from
// the SQL driving

class
    UpdateStatusRecord {
    String sub;
    String primaryUri;
    String docUri;
    String templateName;
    String currentStatus;
    String newStatus;
    int srcId;
    DateTime lastDate;

    UpdateStatusRecord(String sub, String primaryUri, String templateName, String currentStatus) {
      this.sub = sub;
      this.primaryUri = primaryUri;
      this.currentStatus = currentStatus;
      this.templateName = templateName;
    }

    UpdateStatusRecord(String sub, String primaryUri, String currentStatus, DateTime lastDate) {
      this.sub = sub;
      this.primaryUri = primaryUri;
      this.currentStatus = currentStatus;
      this.lastDate = lastDate;
    }

    UpdateStatusRecord(String sub, String primaryUri, String docUri,
                        String currentStatus, DateTime lastDate) {
      this.sub = sub;
      this.primaryUri = primaryUri;
      this.docUri = docUri;
      this.currentStatus = currentStatus;
      this.lastDate = lastDate;
    }

    UpdateStatusRecord() {
    }

    public void setLastDate(String str) {
      lastDate = new DateTime(str);
    }

    public void setDocUri(String uri) {
      this.docUri = uri;
    }

    public String getDocUri() {
      return docUri;
    }

    public void setTemplateName(String name) {
      this.templateName = name;
    }

    public String getTemplateName() {
      return templateName;
    }

    public int getSrcId() {
      return srcId;
    }

    public void setSrcId(int id) {
      this.srcId = id;
    }

    public Object clone() {
      UpdateStatusRecord o = new UpdateStatusRecord();

      o.sub = this.sub;
      o.primaryUri = this.primaryUri;
      o.docUri = this.docUri;
      o.currentStatus = this.currentStatus;
      o.newStatus = this.newStatus;
      o.srcId = this.srcId;
      o.lastDate = this.lastDate;
      o.templateName = this.templateName;

      return o;
    }

    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append(sub);
      sb.append(" ");
      sb.append(primaryUri);
      sb.append(" ");
      sb.append(docUri);
      sb.append(" ");
      sb.append(templateName);
      sb.append(" ");
      sb.append(currentStatus);
      sb.append(" ");
      sb.append(newStatus);
      sb.append(" ");
      sb.append(srcId);
      if (lastDate != null) {
        sb.append(" ");
        sb.append(lastDate.toString());
      }

      return sb.toString();
    }

  }

