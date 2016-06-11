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


package test.utilities;

import java.util.HashMap;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

public class ParseDateTimeField
    extends BaseParseField {

    // field_type:
    //  range_explicit
    //  range_variable
    //  current_time
    boolean useCurrentTime = false;
    boolean useRangeVariable = false;
    boolean useRangeExplicit = false;

    // range_before_current_time
    // the amount of time before now that the time can vary
    long rangeBeforeCurrentTime = 0L;
    //
    // range_after_current_time
    // the amount of time after now that the time can vary
    long rangeAfterCurrentTime = 0L;
    //
    // use_current_time
    // use current time explicitly
    //
    // range_start_time
    // explicit earliest data
    long rangeStartTime = 0L;
    boolean rangeStartTimeNow = false;
    //
    // range_end_time
    // explicit latest date
    long  rangeEndTime = 0L;
    boolean rangeEndTimeNow = false;
    //
    // format_string
    // format string to use
    //
    String formatString;
    SimpleDateFormat formatter = null;


  /**
   * we need a no-arg default constructor in order to be loaded by the
   * class loader
   */
  public ParseDateTimeField() {
    super();
  }

  public void initialize(ParseFieldManager manager, Node t) {
    String tmp;

    parseFieldManager = manager;

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
      if (tagName.equals("field_type")) {
        tmp = getNodeText((Element) children.item(i));
        if (tmp.equalsIgnoreCase("range_variable"))
          useRangeVariable = true;
        else if (tmp.equalsIgnoreCase("range_explicit"))
          useRangeExplicit = true;
        else if (tmp.equalsIgnoreCase("use_current_time"))
          useCurrentTime = true;
      }
      if (tagName.equals("use_current_time")) {
        tmp = getNodeText((Element) children.item(i));
        useCurrentTime = true;
      }
      if (tagName.equals("range_variable")) {
        tmp = getNodeText((Element) children.item(i));
        useRangeVariable = true;
      }
      if (tagName.equals("range_explicit")) {
        tmp = getNodeText((Element) children.item(i));
        useRangeExplicit = true;
      }
      else if (tagName.equals("range_start_time")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals(""))) {
          // System.out.println("range_start_time is " + tmp);
          try {
            if (tmp.equalsIgnoreCase("now")) {
              rangeStartTimeNow = true;
            } else {
              SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
              Date d = f.parse(tmp);
              rangeStartTime = d.getTime();
            }
          } catch (ParseException e) {
            e.printStackTrace();
          }
        }
      }
      else if (tagName.equals("range_end_time")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals(""))) {
          // System.out.println("range_end_time is " + tmp);
          try {
            if (tmp.equalsIgnoreCase("now")) {
              rangeEndTimeNow = true;
            } else {
              SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
              Date d = f.parse(tmp);
              rangeEndTime = d.getTime();
            }
          } catch (ParseException e) {
            e.printStackTrace();
          }
        }
      }
      else if (tagName.equals("range_before_current_time")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals("")))
          rangeBeforeCurrentTime = Long.parseLong(tmp);
      }
      else if (tagName.equals("range_after_current_time")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals("")))
          rangeAfterCurrentTime = Long.parseLong(tmp);
      }
      else if (tagName.equals("format_string")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals(""))) {
          formatString = tmp;
          formatter = new SimpleDateFormat(formatString);
        }
      }

    }
  }

  /**
   * return the desired data. 
   */
  public String generateData(String token) {

    boolean yn = parseFieldManager.getRandomNumberGenerator().nextBoolean();

    String str = null;
    Date d = null;

    if (formatter == null) {
      System.out.println("Fatal error:  formatter is null for token " + token);
    }

    if (useCurrentTime) {
      d = new Date();
      str = formatter.format(d);
    } else if (useRangeVariable) {
      // System.out.println("token is " + token);
      long val = rangeBeforeCurrentTime - 1;
      while ((val < rangeBeforeCurrentTime) &&
              (val > rangeAfterCurrentTime)) {
        val = parseFieldManager.getRandomNumberGenerator().nextLong();
      }
      d = new Date(System.currentTimeMillis() + val);
      str = formatter.format(d);
    } else if (useRangeExplicit) {
      long val = rangeStartTime - 1L;
      long lowVal = rangeStartTime;
      long highVal = rangeEndTime;
      if (rangeStartTimeNow == true)
        lowVal = System.currentTimeMillis();
      if (rangeEndTimeNow == true)
        highVal = System.currentTimeMillis();
      if (lowVal == highVal)
        lowVal -= 1L;
      // TODO:  special case:  if they set the high water mark to "now" we need to
      //fetch a new value for this
      while ((val < lowVal) &&
              (val > highVal)) {
        val = parseFieldManager.getRandomNumberGenerator().nextLong();
      }
      d = new Date(val);
      str = formatter.format(d);
    } else {
      str = "unknown";
    }

    return str;
  }

  /**
   * return the desired data. There is nothing here that calls for context data
   */
  public String generateData(String token, HashMap contextData) {

    return generateData(token);
  }

}

