package com.indeed.squall.iql2.execution.groupkeys;

public interface GroupKeyCreator {
    int parent(int group);
    GroupKey forIndex(int group);
}
