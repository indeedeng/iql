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

package com.indeed.iql2.execution.groupkeys;

import java.util.List;
import java.util.Objects;

public class RangeGroupKey extends GroupKey {
    private final long minInclusive;
    private final long maxExclusive;
    private final String rendered;

    public RangeGroupKey(long minInclusive, long maxExclusive) {
        this.minInclusive = minInclusive;
        this.maxExclusive = maxExclusive;
        rendered = "[" + minInclusive + ", " + maxExclusive + ")";
    }

    @Override
    public void addToList(List<String> list) {
        list.add(rendered);
    }

    @Override
    public String render() {
        return rendered;
    }

    @Override
    public boolean isDefault() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RangeGroupKey that = (RangeGroupKey) o;
        return minInclusive == that.minInclusive &&
                maxExclusive == that.maxExclusive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minInclusive, maxExclusive);
    }

    @Override
    public String toString() {
        return "RangeGroupKey{" +
                "minInclusive=" + minInclusive +
                ", maxExclusive=" + maxExclusive +
                '}';
    }
}
