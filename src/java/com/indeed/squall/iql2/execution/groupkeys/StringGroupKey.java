package com.indeed.squall.iql2.execution.groupkeys;

import java.util.List;

public class StringGroupKey extends GroupKey {
    private final String term;

    public StringGroupKey(String term) {
        this.term = term;
    }

    @Override
    public void addToList(List<String> list) {
        list.add(term);
    }
}
