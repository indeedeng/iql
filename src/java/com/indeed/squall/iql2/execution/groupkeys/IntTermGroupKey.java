package com.indeed.squall.iql2.execution.groupkeys;

import java.util.List;

public class IntTermGroupKey extends GroupKey {
    private final long value;

    public IntTermGroupKey(long value) {
        this.value = value;
    }

    @Override
    public void addToList(List<String> list) {
        list.add(String.valueOf(value));
    }
}
