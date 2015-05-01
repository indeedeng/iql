package com.indeed.squall.jql;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Pushable {
    Set<QualifiedPush> requires();
    void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys);
}
