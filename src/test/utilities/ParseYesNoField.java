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
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

public class ParseYesNoField
    extends BaseParseField {

    boolean useMixedCase = false;
    boolean useAbbreviation = false;

  /**
   * we need a no-arg default constructor in order to be loaded by the
   * class loader
   */
  public ParseYesNoField() {
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

      // decide if we're returning YES/NO or Yes/No
      else if (tagName.equals("use_mixed_case")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals("")))
          useMixedCase = Boolean.parseBoolean(tmp);
        // if we're using mixed case, let's assume we aren't abbreviating for now
        useAbbreviation = false;
      }
      // decide if we're returning Y/N or something longer
      else if (tagName.equals("use_abbreviation")) {
        tmp = getNodeText((Element) children.item(i));
        if (!(tmp == null || tmp.equals("")))
          useAbbreviation = Boolean.parseBoolean(tmp);
      }
    }
  }

  /**
   * return the desired data. In this case, the default behavior of the class
   * is to return a Y/N field. If use_mixed_case is set in the configuration,
   * we then switch to returning either YES/NO or Yes/No. Setting use_abbreviation
   * in the configuration makes returning Y/N explicit. Setting both fields in the
   * configuration creates a race condition:  last one processed wins.
   */
  public String generateData(String token) {

    boolean yn = parseFieldManager.getRandomNumberGenerator().nextBoolean();

    String str = null;

    // TODO:  I'm sure we can do useLowerCase to force the response to be "yes" or "no"
    // let's see if we really must. The flags become overwhelming at some point.
    if (useAbbreviation) {
      if (yn)
        str = "Y";
      else
        str = "N";
    } else {
      if (yn) {
        if (useMixedCase)
          str = "Yes";
        else
          str = "YES";
      } else {
        if (useMixedCase)
          str = "No";
        else
          str = "NO";
      }
    }

    return str;
  }

  public String generateData(String token, HashMap contextData) {
    return generateData(token);
  }

}

