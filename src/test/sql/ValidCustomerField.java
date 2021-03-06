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

import java.util.HashMap;
import test.utilities.ParseFieldManager;
import test.utilities.BaseParseField;

import org.w3c.dom.Node;

public class ValidCustomerField
                extends BaseParseField {

  public static final String TOKEN = "__VALID_CUSTOMER__";
  private CustomerRecord record = null;

  public ValidCustomerField() {
    super();
    token = TOKEN;
  }

  public ValidCustomerField(String token) {
    super();
    if (token == null)
      this.token = TOKEN;
    else
      this.token = token;
  }

  public void initialize(ParseFieldManager mgr, Node t) {

    // record = getRandomCustomer();
  }

  public String generateData(String token) {
/*
    if (record == null)
      record = getRandomCustomer();
*/
    record = getRandomCustomer();

    // System.out.println("Returning customer ID of " + record.getCustomerId());

    return Integer.toString(record.getCustomerId());
  }

  public String generateData(String token, HashMap extra) {
    return generateData(token);
  }

  public CustomerRecord getRandomCustomer() {

    CustomerDataManager manager = CustomerDataManager.getTracker();
    CustomerRecord traderRecord = null;

    while (traderRecord == null) {
      try {
        traderRecord = manager.getRandomRecord();
        // traderID.setID(Integer.toString(traderRecord.traderId));
        if (traderRecord == null) {
          System.out.println("traderRecord is null. sleeping a bit.");
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        // do nothing
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return traderRecord;
  }
}


