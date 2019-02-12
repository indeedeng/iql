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
import com.indeed.iql2.execution.groupkeys.DefaultGroupKey;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.HighGutterGroupKey;
import com.indeed.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.iql2.execution.groupkeys.LowGutterGroupKey;
import com.indeed.iql2.execution.groupkeys.RangeGroupKey;

import java.util.Objects;

public class MetricRangeGroupKeySet implements GroupKeySet {
    private final GroupKeySet previous;
    private final int numBuckets;
    private final boolean excludeGutters;
    private final long min;
    private final long interval;
    private final boolean withDefaultBucket;
    private final boolean fromPredicate;
    private final int numGroups;
    private final HighGutterGroupKey highGutterGroupKey;
    private final LowGutterGroupKey lowGutterGroupKey;
    private final LoadingCache<Integer, GroupKey> buildGroupKey;

    public MetricRangeGroupKeySet(
            final GroupKeySet previous,
            final int numBuckets,
            final boolean excludeGutters,
            final long min,
            final long interval,
            final boolean withDefaultBucket,
            final boolean fromPredicate,
            final int numGroups,
            final Formatter formatter) {
        this.previous = previous;
        this.numBuckets = numBuckets;
        this.excludeGutters = excludeGutters;
        this.min = min;
        this.interval = interval;
        this.withDefaultBucket = withDefaultBucket;
        this.fromPredicate = fromPredicate;
        this.numGroups = numGroups;
        this.highGutterGroupKey = new HighGutterGroupKey(min + (interval * (numBuckets - 2)));
        this.lowGutterGroupKey = new LowGutterGroupKey(min);
        this.buildGroupKey = CacheBuilder.newBuilder()
                .build(new CacheLoader<Integer, GroupKey>() {
                    @Override
                    public GroupKey load(final Integer innerGroup) {
                        if (fromPredicate) {
                            return new IntTermGroupKey(innerGroup);
                        } else {
                            final long minInclusive = min + (innerGroup * interval);
                            final long maxExclusive = min + ((innerGroup + 1) * interval);
                            return new RangeGroupKey(minInclusive, maxExclusive, formatter);
                        }
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
        final int innerGroup = (group - 1) % numBuckets;
        if (!excludeGutters && (innerGroup == (numBuckets - 1))) {
            return highGutterGroupKey;
        } else if (!excludeGutters && (innerGroup == (numBuckets - 2))) {
            return lowGutterGroupKey;
        } else if (withDefaultBucket && (innerGroup == (numBuckets - 1))) {
            return DefaultGroupKey.DEFAULT_INSTANCE;
        } else {
            return buildGroupKey.getUnchecked(innerGroup);
        }
    }

    @Override
    public int numGroups() {
        return numGroups;
    }

    @Override
    public boolean isPresent(int group) {
        return group > 0 && group <= numGroups() && previous.isPresent(parentGroup(group));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricRangeGroupKeySet that = (MetricRangeGroupKeySet) o;
        return numBuckets == that.numBuckets &&
                excludeGutters == that.excludeGutters &&
                min == that.min &&
                interval == that.interval &&
                withDefaultBucket == that.withDefaultBucket &&
                fromPredicate == that.fromPredicate &&
                numGroups == that.numGroups &&
                Objects.equals(previous, that.previous);
    }

    @Override
    public int hashCode() {
        return Objects.hash(previous, numBuckets, excludeGutters, min, interval, withDefaultBucket, fromPredicate, numGroups);
    }
}
