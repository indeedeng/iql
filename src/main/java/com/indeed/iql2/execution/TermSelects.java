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

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

import java.util.Arrays;
import java.util.Comparator;

/**
* @author jwolfe
*/
public class TermSelects {
    public final String stringTerm;
    public final long intTerm;

    public final double[] selects;
    public final double topMetric;

    public TermSelects(
            final long intTerm,
            final double[] selects,
            final double topMetric) {
        this.stringTerm = null;
        this.intTerm = intTerm;
        this.selects = selects;
        this.topMetric = topMetric;
    }

    public TermSelects(
            final String stringTerm,
            final double[] selects,
            final double topMetric) {
        this.stringTerm = stringTerm;
        this.intTerm = 0;
        this.selects = selects;
        this.topMetric = topMetric;
    }

    @Override
    public String toString() {
        return "TermSelects{" +
                ", stringTerm='" + stringTerm + '\'' +
                ", intTerm=" + intTerm +
                ", selects=" + Arrays.toString(selects) +
                ", topMetric=" + topMetric +
                '}';
    }

    public static final Comparator<TermSelects> COMPARATOR = new Comparator<TermSelects>() {
        @Override
        public int compare(TermSelects o1, TermSelects o2) {
            final double v1 = Double.isNaN(o1.topMetric) ? Double.NEGATIVE_INFINITY : o1.topMetric;
            final double v2 = Double.isNaN(o2.topMetric) ? Double.NEGATIVE_INFINITY : o2.topMetric;

            int r = Doubles.compare(v1, v2);
            if (r != 0) {
                return r;
            }

            if (o1.stringTerm == null) {
                r = Longs.compare(o2.intTerm, o1.intTerm);
            } else {
                r = o2.stringTerm.compareTo(o1.stringTerm);
            }
            return r;
        }
    };
}
