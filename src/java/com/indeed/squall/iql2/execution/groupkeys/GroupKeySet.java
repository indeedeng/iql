package com.indeed.squall.iql2.execution.groupkeys;

import java.util.List;

public interface GroupKeySet {
    List<String> asList(int group);
    GroupKeySet previous();
    int parentGroup(int group);
    GroupKey groupKey(int group);
    int numGroups();
    boolean isPresent(int group);
}
