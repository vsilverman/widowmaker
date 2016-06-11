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

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MinMaxValidCustomerField
                extends ValidCustomerField {

  public static final String TOKEN = "__MIN_VALID_CUSTOMER__";
  public static final String MAX_VALID_CUSTOMER_TOKEN = "__MAX_VALID_CUSTOMER__";
  public static final String MIN_VALID_CUSTOMER_TOKEN = "__MIN_VALID_CUSTOMER__";

  public static final String EXTRA_MIN_TOKEN = "MinCustomer";
  public static final String EXTRA_MAX_TOKEN = "MaxCustomer";

  private CustomerRecord record = null;

  public MinMaxValidCustomerField() {
    super();
    token = TOKEN;
  }

  public MinMaxValidCustomerField(String token) {
    super(token);
    if (token == null)
      this.token = TOKEN;
    else
      this.token = token;
  }

  public void initialize(ParseFieldManager mgr, Node t) {
    String tmp;

    // we have multiple token handlers in here in a single class. We need to tuck
    // away the token from the config file so that we can respond both ways

    if ((t == null) || (t.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE))
      return;
    NodeList children = t.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      if (children.item(i).getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
        continue;
      }
      String tagName = ((Element) children.item(i)).getTagName();
      if (tagName.equals("token")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals(""))) {
          token = tmp;
        }
      }
/*
      if (tagName.equals("min")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals("")))
          minValue = Integer.parseInt(tmp);
      }
      else if (tagName.equals("max")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals("")))
          maxValue = Integer.parseInt(tmp);
      }
*/
    }

  }

  public String generateData(String token) {
    record = getRandomCustomer();

    // System.out.println("Returning customer ID of " + record.getCustomerId());

    return Integer.toString(record.getCustomerId());
  }

  public String generateData(String token, HashMap extra) {

    int minCustomer = -1;
    int maxCustomer = -1;

    // System.out.println("MinMaxValidCustomer:  " + token);

    // some funky logic
    Object obj;

    obj = extra.get(EXTRA_MIN_TOKEN);
    if (obj != null)
      minCustomer = ((Integer)obj).intValue();
    obj = extra.get(EXTRA_MAX_TOKEN);
    if (obj != null)
      maxCustomer = ((Integer)obj).intValue();

    // System.out.println("current minCustomer:  " + minCustomer);
    // System.out.println("current maxCustomer:  " + maxCustomer);

    if (token.equals(MIN_VALID_CUSTOMER_TOKEN)) {
      // if we already have a max, we just need to come out lower
      if (maxCustomer > -1) {
        while ((minCustomer == -1)
          || (minCustomer > maxCustomer)) {
          record = getRandomCustomer();
          minCustomer = record.getCustomerId();
        }
      } else {
        record = getRandomCustomer();
        minCustomer = record.getCustomerId();
      }

      obj = new Integer(minCustomer);
      extra.put(EXTRA_MIN_TOKEN, obj);
      // System.out.println("returning minCustomer of " + obj);
      return Integer.toString(minCustomer);
    } else {
      if (minCustomer > -1) {
        while ((maxCustomer == -1)
          || (maxCustomer < minCustomer)) {
          record = getRandomCustomer();
          maxCustomer = record.getCustomerId();
        }
      } else {
        record = getRandomCustomer();
        maxCustomer = record.getCustomerId();
      }

      obj = new Integer(maxCustomer);
      extra.put(EXTRA_MAX_TOKEN, obj);
      // System.out.println("returning maxCustomer of " + obj);
      return Integer.toString(maxCustomer);
    }


  }

  public CustomerRecord getRandomCustomer() {

    CustomerDataManager manager = CustomerDataManager.getTracker();
    CustomerRecord customerRecord = null;

    while (customerRecord == null) {
      try {
        customerRecord = manager.getRandomRecord();
        // traderID.setID(Integer.toString(customerRecord.traderId));
        if (customerRecord == null) {
          System.out.println("customerRecord is null. sleeping a bit.");
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        // do nothing
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return customerRecord;
  }
}


