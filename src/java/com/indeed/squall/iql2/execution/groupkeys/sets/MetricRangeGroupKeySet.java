package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.groupkeys.DefaultGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.HighGutterGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.LowGutterGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.RangeGroupKey;

import java.util.Objects;

public class MetricRangeGroupKeySet implements GroupKeySet {
    private final GroupKeySet previous;
    private final int numBuckets;
    private final boolean excludeGutters;
    private final long min;
    private final long interval;
    private final boolean withDefaultBucket;
    private final boolean fromPredicate;

    public MetricRangeGroupKeySet(GroupKeySet previous, int numBuckets, boolean excludeGutters, long min, long interval, boolean withDefaultBucket, boolean fromPredicate) {
        this.previous = previous;
        this.numBuckets = numBuckets;
        this.excludeGutters = excludeGutters;
        this.min = min;
        this.interval = interval;
        this.withDefaultBucket = withDefaultBucket;
        this.fromPredicate = fromPredicate;
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
        if (!excludeGutters && innerGroup == numBuckets - 1) {
            return new HighGutterGroupKey(min + interval * (numBuckets - 2));
        } else if (!excludeGutters && innerGroup == numBuckets - 2) {
            return new LowGutterGroupKey(min);
        } else if (withDefaultBucket && innerGroup == numBuckets - 1) {
            return DefaultGroupKey.DEFAULT_INSTANCE;
        } else if (fromPredicate) {
            return new IntTermGroupKey(innerGroup);
        } else {
            final long minInclusive = min + innerGroup * interval;
            final long maxExclusive = min + (innerGroup + 1) * interval;
            return new RangeGroupKey(minInclusive, maxExclusive);
        }
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
        MetricRangeGroupKeySet that = (MetricRangeGroupKeySet) o;
        return numBuckets == that.numBuckets &&
                excludeGutters == that.excludeGutters &&
                min == that.min &&
                interval == that.interval &&
                withDefaultBucket == that.withDefaultBucket &&
                fromPredicate == this.fromPredicate &&
                Objects.equals(previous, that.previous);
    }

    @Override
    public int hashCode() {
        return Objects.hash(previous, numBuckets, excludeGutters, min, interval, withDefaultBucket, fromPredicate);
    }
}
