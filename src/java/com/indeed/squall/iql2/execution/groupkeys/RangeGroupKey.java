package com.indeed.squall.iql2.execution.groupkeys;

import java.util.List;

public class RangeGroupKey extends GroupKey {
    private final long minInclusive;
    private final long maxExclusive;

    public RangeGroupKey(long minInclusive, long maxExclusive) {
        this.minInclusive = minInclusive;
        this.maxExclusive = maxExclusive;
    }

    @Override
    public void addToList(List<String> list) {
        list.add("[" + minInclusive + ", " + maxExclusive + ")");
    }
}
