package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.groupkeys.DayRangeGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;

public class DateTimeRangeGroupKeySet implements GroupKeySet {
    private final GroupKeySet previous;
    private final long earliestStart;
    private final long periodMillis;
    private final int numBuckets;
    private final String format;

    public DateTimeRangeGroupKeySet(GroupKeySet previous, long earliestStart, long periodMillis, int numBuckets, String format) {
        this.previous = previous;
        this.earliestStart = earliestStart;
        this.periodMillis = periodMillis;
        this.numBuckets = numBuckets;
        this.format = format;
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
        final long start = earliestStart + groupOffset * periodMillis;
        final long end = earliestStart + (groupOffset + 1) * periodMillis;
        return new DayRangeGroupKey(format, start, end);
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
