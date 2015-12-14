package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.HighGutterGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.LowGutterGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.RangeGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.SingleValueGroupKey;

public class MetricRangeGroupKeySet implements GroupKeySet {
    private final GroupKeySet previous;
    private final int numBuckets;
    private final boolean excludeGutters;
    private final long min;
    private final long interval;

    public MetricRangeGroupKeySet(GroupKeySet previous, int numBuckets, boolean excludeGutters, long min, long interval) {
        this.previous = previous;
        this.numBuckets = numBuckets;
        this.excludeGutters = excludeGutters;
        this.min = min;
        this.interval = interval;
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
        } else {
            if (interval == 1) {
                return new SingleValueGroupKey(min + innerGroup);
            } else {
                final long minInclusive = min + innerGroup * interval;
                final long maxExclusive = min + (innerGroup + 1) * interval;
                return new RangeGroupKey(minInclusive, maxExclusive);
            }
        }
    }

    @Override
    public int numGroups() {
        return previous.numGroups() * numBuckets;
    }

    @Override
    public boolean isPresent(int group) {
        return group <= numGroups() && previous.isPresent(parentGroup(group));
    }
}
