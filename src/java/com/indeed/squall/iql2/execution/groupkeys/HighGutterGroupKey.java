package com.indeed.squall.iql2.execution.groupkeys;

import com.indeed.squall.iql2.execution.Session;

import java.util.List;
import java.util.Objects;

public class HighGutterGroupKey extends GroupKey {
    private final long start;

    public HighGutterGroupKey(long start) {
        this.start = start;
    }

    @Override
    public void addToList(List<String> list) {
        list.add("[" + start + ", " + Session.INFINITY_SYMBOL + ")");
    }

    @Override
    public boolean isDefault() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HighGutterGroupKey that = (HighGutterGroupKey) o;
        return start == that.start;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start);
    }

    @Override
    public String toString() {
        return "HighGutterGroupKey{" +
                "start=" + start +
                '}';
    }
}
