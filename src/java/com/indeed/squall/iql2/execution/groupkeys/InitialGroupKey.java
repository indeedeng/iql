package com.indeed.squall.iql2.execution.groupkeys;

import java.util.List;

public class InitialGroupKey extends GroupKey {
    public static final InitialGroupKey INSTANCE = new InitialGroupKey();

    private InitialGroupKey() {}

    @Override
    public void addToList(List<String> list) {
    }
}
