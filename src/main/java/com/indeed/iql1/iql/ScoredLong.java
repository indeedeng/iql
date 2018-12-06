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

package com.indeed.iql1.iql;

import com.google.common.primitives.Doubles;

import java.util.Comparator;

/**
 * @author jsadun
 */
public class ScoredLong {
    // do a custom comparator to ensure that real numbers are preferred to NaNs
    public static final Comparator<ScoredLong> TOP_SCORE_COMPARATOR = new Comparator<ScoredLong>() {
        @Override
        public int compare(final ScoredLong o1, final ScoredLong o2) {
            final int r = -Doubles.compare(-o1.getScore(), -o2.getScore()); // considers NaN as the lowest score value
            if (r != 0) {
                return r;
            }
            return Long.compare(o1.value, o2.value);
        }
    };

    public static final Comparator<ScoredLong> BOTTOM_SCORE_COMPARATOR = new Comparator<ScoredLong>() {
        @Override
        public int compare(final ScoredLong o1, final ScoredLong o2) {
            final int r = -Doubles.compare(o1.getScore(), o2.getScore()); // considers NaN as the highest score value
            if (r != 0) {
                return r;
            }
            return -Long.compare(o1.value, o2.value);
        }
    };

    private final double score;
    private final long value;

    public ScoredLong(final double score, final long value) {
        this.score = score;
        this.value = value;
    }

    public double getScore() {
        return score;
    }

    public long getValue() {
        return value;
    }
}
