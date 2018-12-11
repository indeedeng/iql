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

import java.util.Objects;

public class LowGutterGroupKey extends GroupKey {
    private final long min;

    public LowGutterGroupKey(long min) {
        this.min = min;
    }

    @Override
    public String render() {
        return "< " + min;
    }

    @Override
    public boolean isDefault() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LowGutterGroupKey that = (LowGutterGroupKey) o;
        return min == that.min;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min);
    }

    @Override
    public String toString() {
        return "LowGutterGroupKey{" +
                "min=" + min +
                '}';
    }
}
