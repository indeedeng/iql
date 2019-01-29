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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.util.Objects;

public class DateTimeRangeGroupKeySet implements GroupKeySet {
    private final GroupKeySet previous;
    private final long earliestStart;
    private final long periodMillis;
    private final int numBuckets;
    private final String format;

    private final LoadingCache<Integer, StringGroupKey> buildGroupKey;

    public DateTimeRangeGroupKeySet(GroupKeySet previous, long earliestStart, long periodMillis, int numBuckets, String format, final Formatter formatter) {
        this.previous = previous;
        this.earliestStart = earliestStart;
        this.periodMillis = periodMillis;
        this.numBuckets = numBuckets;
        this.format = format;
        final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(format).withLocale(Locale.US);
        buildGroupKey = CacheBuilder.newBuilder()
                .build(new CacheLoader<Integer, StringGroupKey>() {
                    @Override
                    public StringGroupKey load(final Integer groupOffset) {
                        final long start = earliestStart + groupOffset * periodMillis;
                        final long end = earliestStart + (groupOffset + 1) * periodMillis;
                        return StringGroupKey.fromTimeRange(dateTimeFormatter, start, end, formatter);
                    }
                });
    }

    @Override
    public GroupKeySet previous() {
        return previous;
    }

    @Override
    public int parentGroup(int group) {
        return 1 + (group - 1) / numBuckets;
    }

    @Override
    public GroupKey groupKey(int group) {
        final int oldGroup = this.parentGroup(group);
        final int groupOffset = group - 1 - ((oldGroup - 1) * numBuckets);
        return buildGroupKey.getUnchecked(groupOffset);
    }

    @Override
    public int numGroups() {
        return previous.numGroups() * numBuckets;
    }

    @Override
    public boolean isPresent(int group) {
        return group > 0 && group <= numGroups() && previous.isPresent(parentGroup(group));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateTimeRangeGroupKeySet that = (DateTimeRangeGroupKeySet) o;
        return earliestStart == that.earliestStart &&
                periodMillis == that.periodMillis &&
                numBuckets == that.numBuckets &&
                Objects.equals(previous, that.previous) &&
                Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(previous, earliestStart, periodMillis, numBuckets, format);
    }
}
