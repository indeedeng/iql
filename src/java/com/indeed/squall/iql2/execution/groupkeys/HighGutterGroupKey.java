package com.indeed.squall.iql2.execution.groupkeys;

import com.indeed.squall.iql2.execution.Session;

import java.util.List;

public class HighGutterGroupKey extends GroupKey {
    private final long start;

    public HighGutterGroupKey(long start) {
        this.start = start;
    }

    @Override
    public void addToList(List<String> list, boolean appendingTerm) {
        list.add("[" + start + ", " + Session.INFINITY_SYMBOL + ")");
    }
}
