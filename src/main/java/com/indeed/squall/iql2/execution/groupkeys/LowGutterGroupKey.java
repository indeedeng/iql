package com.indeed.squall.iql2.execution.groupkeys;

import java.util.List;
import java.util.Objects;

public class LowGutterGroupKey extends GroupKey {
    private final long min;

    public LowGutterGroupKey(long min) {
        this.min = min;
    }

    @Override
    public void addToList(List<String> list) {
        list.add("< " + min);
    }

    @Override
    public boolean isDefault() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LowGutterGroupKey that = (LowGutterGroupKey) o;
        return min == that.min;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min);
    }

    @Override
    public String toString() {
        return "LowGutterGroupKey{" +
                "min=" + min +
                '}';
    }
}
