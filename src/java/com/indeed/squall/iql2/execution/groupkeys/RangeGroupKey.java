package com.indeed.squall.iql2.execution.groupkeys;

import java.util.List;
import java.util.Objects;

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

    @Override
    public boolean isDefault() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RangeGroupKey that = (RangeGroupKey) o;
        return minInclusive == that.minInclusive &&
                maxExclusive == that.maxExclusive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minInclusive, maxExclusive);
    }

    @Override
    public String toString() {
        return "RangeGroupKey{" +
                "minInclusive=" + minInclusive +
                ", maxExclusive=" + maxExclusive +
                '}';
    }
}
