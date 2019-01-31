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

import com.indeed.iql2.Formatter;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Objects;

@EqualsAndHashCode(callSuper = false)
@ToString
public class RangeGroupKey extends GroupKey {
    private final long minInclusive;
    private final long maxExclusive;
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final String rendered;

    public RangeGroupKey(long minInclusive, long maxExclusive, final Formatter formatter) {
        this.minInclusive = minInclusive;
        this.maxExclusive = maxExclusive;
        rendered = formatter.escape("[" + minInclusive + ", " + maxExclusive + ")");
    }

    @Override
    public String render() {
        return rendered;
    }

    @Override
    public boolean isDefault() {
        return false;
    }
}
