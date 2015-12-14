package com.indeed.squall.iql2.execution.groupkeys;

import com.indeed.squall.iql2.execution.Session;

import java.util.List;

public class LowGutterGroupKey extends GroupKey {
    private final long min;

    public LowGutterGroupKey(long min) {
        this.min = min;
    }

    @Override
    public void addToList(List<String> list) {
        list.add("[-" + Session.INFINITY_SYMBOL + ", " + min + ")");
    }
}
