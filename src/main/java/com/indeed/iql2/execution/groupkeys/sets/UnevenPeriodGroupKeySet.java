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
import com.indeed.iql2.language.query.UnevenGroupByPeriod;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;

@EqualsAndHashCode
@ToString
public class UnevenPeriodGroupKeySet implements GroupKeySet {
    private final GroupKeySet previous;
    private final int numPeriods;
    private final DateTime start;
    private final UnevenGroupByPeriod groupByType;
    private final String formatString;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final LoadingCache<Integer, StringGroupKey> buildGroupKey;

    public UnevenPeriodGroupKeySet(
            final GroupKeySet previous,
            final int numPeriods,
            final DateTime start,
            final UnevenGroupByPeriod groupByType,
            final String formatString,
            final Formatter formatter) {
        this.previous = previous;
        this.numPeriods = numPeriods;
        this.start = start;
        this.groupByType = groupByType;
        this.formatString = formatString;
        final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(formatString).withLocale(Locale.US);
        buildGroupKey = CacheBuilder.newBuilder()
                .build(new CacheLoader<Integer, StringGroupKey>() {
                    @Override
                    public StringGroupKey load(final Integer periodOffset) {
                        final DateTime periodStart = groupByType.plusPeriods(start, periodOffset);
                        final DateTime periodEnd = groupByType.plusPeriods(periodStart, 1);
                        return StringGroupKey.fromTimeRange(dateTimeFormatter, periodStart.getMillis(), periodEnd.getMillis(), formatter);
                    }
                });
    }

    @Override
    public GroupKeySet previous() {
        return previous;
    }

    @Override
    public int parentGroup(final int group) {
        return 1 + (group - 1) / numPeriods;
    }

    @Override
    public GroupKey groupKey(final int group) {
        final int periodOffset = (group - 1) % numPeriods;
        return buildGroupKey.getUnchecked(periodOffset);
    }

    @Override
    public int numGroups() {
        return previous.numGroups() * numPeriods;
    }

    @Override
    public boolean isPresent(final int group) {
        return group > 0 && group <= numGroups() && previous.isPresent(parentGroup(group));
    }
}
