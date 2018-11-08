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

package com.indeed.iql2.execution.groupkeys.sets;

import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.iql2.execution.groupkeys.TimeRangeGroupKey;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.util.Objects;

public class YearMonthGroupKey implements GroupKeySet {
    private final GroupKeySet previous;
    private final int numMonths;
    private final DateTime startMonth;
    private final String formatString;
    private final DateTimeFormatter formatter;
    private final boolean useTimeRangeKeys;

    public YearMonthGroupKey(
            final GroupKeySet previous,
            final int numMonths,
            final DateTime startMonth,
            final String formatString,
            final boolean useTimeRangeKeys
    ) {
        this.previous = previous;
        this.numMonths = numMonths;
        this.startMonth = startMonth;
        this.formatString = formatString;
        this.formatter = DateTimeFormat.forPattern(formatString).withLocale(Locale.US);
        this.useTimeRangeKeys = useTimeRangeKeys;
    }

    @Override
    public GroupKeySet previous() {
        return previous;
    }

    @Override
    public int parentGroup(int group) {
        return 1 + (group - 1) / numMonths;
    }

    @Override
    public GroupKey groupKey(int group) {
        final int monthOffset = (group - 1) % numMonths;
        final DateTime month = startMonth.plusMonths(monthOffset);
        if (useTimeRangeKeys) {
            return new TimeRangeGroupKey(formatString, month.getMillis(), month.plusMonths(1).getMillis());
        } else {
            return new StringGroupKey(formatter.print(month));
        }
    }

    @Override
    public int numGroups() {
        return previous.numGroups() * numMonths;
    }

    @Override
    public boolean isPresent(int group) {
        return group > 0 && group <= numGroups() && previous.isPresent(parentGroup(group));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        YearMonthGroupKey that = (YearMonthGroupKey) o;
        return numMonths == that.numMonths &&
                Objects.equals(previous, that.previous) &&
                Objects.equals(startMonth, that.startMonth) &&
                Objects.equals(formatString, that.formatString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(previous, numMonths, startMonth, formatString);
    }
}
