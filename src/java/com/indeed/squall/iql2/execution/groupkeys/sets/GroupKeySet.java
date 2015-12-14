package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.groupkeys.GroupKey;

public interface GroupKeySet {
    GroupKeySet previous();
    int parentGroup(int group);
    GroupKey groupKey(int group);
    int numGroups();
    boolean isPresent(int group);
}
