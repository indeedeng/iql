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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.indeed.iql2.Formatter;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.StringGroupKey;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;

@EqualsAndHashCode
@ToString
public class YearMonthGroupKeySet implements GroupKeySet {
    private final GroupKeySet previous;
    private final int numMonths;
    private final DateTime startMonth;
    private final String formatString;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final LoadingCache<DateTime, StringGroupKey> buildGroupKey;

    public YearMonthGroupKeySet(
            final GroupKeySet previous,
            final int numMonths,
            final DateTime startMonth,
            final String formatString,
            final Formatter formatter) {
        this.previous = previous;
        this.numMonths = numMonths;
        this.startMonth = startMonth;
        this.formatString = formatString;
        final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(formatString).withLocale(Locale.US);
        buildGroupKey = CacheBuilder.newBuilder()
                .build(new CacheLoader<DateTime, StringGroupKey>() {
                    @Override
                    public StringGroupKey load(final DateTime month) {
                        return StringGroupKey.fromTimeRange(dateTimeFormatter, month.getMillis(), month.plusMonths(1).getMillis(), formatter);
                    }
                });
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
        return buildGroupKey.getUnchecked(month);
    }

    @Override
    public int numGroups() {
        return previous.numGroups() * numMonths;
    }

    @Override
    public boolean isPresent(int group) {
        return group > 0 && group <= numGroups() && previous.isPresent(parentGroup(group));
    }
}
