package com.indeed.squall.iql2.execution.groupkeys;

import java.util.List;

public class DefaultGroupKey extends GroupKey {
    public static final DefaultGroupKey INSTANCE = new DefaultGroupKey();

    private DefaultGroupKey() {
    }

    @Override
    public void addToList(List<String> list) {
        list.add("DEFAULT");
    }
}
