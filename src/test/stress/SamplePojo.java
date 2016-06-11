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

import java.util.Calendar;
import com.marklogic.client.pojo.annotation.Id;

public class SamplePojo implements Cloneable {
    @Id
    public long id;
    public String shortData;
    public Integer integerData;
    public boolean boolData;
    public String longData;
    public Calendar dateData;
    public SamplePojo childData;
    public SamplePojo copy() {
        try {
            return (SamplePojo) clone();
        } catch (CloneNotSupportedException e) { throw new IllegalStateException(e); }
    }
}
