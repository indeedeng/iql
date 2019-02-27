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

import java.util.Comparator;

/**
 * @author jsadun
 */
public final class ScoredObject<T> {
    // do a custom comparator to ensure that real numbers are preferred to NaNs

    public static <T> Comparator<ScoredObject<T>> topScoredObjectComparator(final Comparator<T> comparator) {
        return new Comparator<ScoredObject<T>>() {
            @Override
            public int compare(ScoredObject<T> o1, ScoredObject<T> o2) {
                final int r = Double.compare(-o2.getScore(), -o1.getScore()); // considers NaN as the lowest score value
                if (r != 0) {
                    return r;
                }
                return comparator.compare(o1.getObject(), o2.getObject());
            }
        };
    }

    public static <T> Comparator<ScoredObject<T>> bottomScoredObjectComparator(final Comparator<T> comparator) {
        return new Comparator<ScoredObject<T>>() {
            @Override
            public int compare(ScoredObject<T> o1, ScoredObject<T> o2) {
                final int r = Double.compare(o2.getScore(), o1.getScore()); // considers NaN as the highest score value
                if (r != 0) {
                    return r;
                }
                return -comparator.compare(o1.getObject(), o2.getObject());
            }
        };
    }

    private final double score;
    private final T object;

    public ScoredObject(final double score, final T object) {
        this.score = score;
        this.object = object;
    }

    public double getScore() {
        return score;
    }

    public T getObject() {
        return object;
    }
}
