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

package com.indeed.squall.iql2.execution.groupkeys;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class TimeRangeGroupKey extends GroupKey {
    private final String format;
    private final long start;
    private final long end;
    private final DateTimeFormatter formatter;

    public TimeRangeGroupKey(String format, long start, long end) {
        this.format = format;
        this.start = start;
        this.end = end;
        formatter = DateTimeFormat.forPattern(format).withLocale(Locale.US);
    }

    @Override
    public void addToList(List<String> list) {
        list.add("[" + formatter.print(start) + ", " + formatter.print(end) + ")");
    }

    @Override
    public boolean isDefault() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeRangeGroupKey that = (TimeRangeGroupKey) o;
        return start == that.start &&
                end == that.end &&
                Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, start, end);
    }

    @Override
    public String toString() {
        return "TimeRangeGroupKey{" +
                "format='" + format + '\'' +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
