/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.execution;

import java.util.Arrays;

/**
* @author jwolfe
*/
public class TermSelects {
    public final String field;

    public boolean isIntTerm;
    public String stringTerm;
    public long intTerm;

    public final double[] selects;
    public double topMetric;
    public final int group;

    public TermSelects(String field, boolean isIntTerm, String stringTerm, long intTerm, double[] selects, double topMetric, int group) {
        this.field = field;
        this.stringTerm = stringTerm;
        this.intTerm = intTerm;
        this.isIntTerm = isIntTerm;
        this.selects = selects;
        this.topMetric = topMetric;
        this.group = group;
    }

    @Override
    public String toString() {
        return "TermSelects{" +
                "isIntTerm=" + isIntTerm +
                ", stringTerm='" + stringTerm + '\'' +
                ", intTerm=" + intTerm +
                ", selects=" + Arrays.toString(selects) +
                ", topMetric=" + topMetric +
                ", group=" + group +
                '}';
    }
}
