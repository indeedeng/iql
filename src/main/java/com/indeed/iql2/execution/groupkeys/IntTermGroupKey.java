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

public class IntTermGroupKey extends GroupKey {
    public final long value;

    public IntTermGroupKey(long value) {
        this.value = value;
    }

    @Override
    public void addToList(List<String> list) {
        list.add(String.valueOf(value));
    }

    @Override
    public String render() {
        return String.valueOf(value);
    }

    @Override
    public void appendWithTab(final StringBuilder sb) {
        sb.append(value);
        sb.append('\t');
    }

    @Override
    public boolean isDefault() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntTermGroupKey that = (IntTermGroupKey) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "IntTermGroupKey{" +
                "value=" + value +
                '}';
    }
}
