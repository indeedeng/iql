package com.indeed.squall.iql2.execution.groupkeys;

import java.util.List;

public class DumbGroupKey extends GroupKey {
    private final String term;

    public DumbGroupKey(String term) {
        this.term = term;
    }

    @Override
    public void addToList(List<String> list) {
        list.add(term);
    }
}
