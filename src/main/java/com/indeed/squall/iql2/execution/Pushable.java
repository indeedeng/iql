package com.indeed.squall.iql2.execution;

import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Map;
import java.util.Set;

public interface Pushable {
    Set<QualifiedPush> requires();
    void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet);
}
