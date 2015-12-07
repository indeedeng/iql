package com.indeed.squall.iql2.execution.groupkeys;

import java.util.List;

public abstract class GroupKey {
    public abstract void addToList(List<String> list, boolean appendingTerm);
}
