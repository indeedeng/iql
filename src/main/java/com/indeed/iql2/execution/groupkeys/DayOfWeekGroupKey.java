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

import com.indeed.iql2.execution.commands.ExplodeDayOfWeek;

import java.util.Objects;

public class DayOfWeekGroupKey extends GroupKey {
    private final int dayOfWeek;

    public DayOfWeekGroupKey(int dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
    }

    @Override
    public String render() {
        return ExplodeDayOfWeek.DAY_KEYS[dayOfWeek];
    }

    @Override
    public boolean isDefault() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DayOfWeekGroupKey that = (DayOfWeekGroupKey) o;
        return dayOfWeek == that.dayOfWeek;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dayOfWeek);
    }

    @Override
    public String toString() {
        return "DayOfWeekGroupKey{" +
                "dayOfWeek=" + dayOfWeek +
                '}';
    }
}
