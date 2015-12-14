package com.indeed.squall.iql2.execution.groupkeys;

import com.google.common.collect.Lists;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Collections;
import java.util.List;

public class GroupKeySets {
    public static List<String> asList(GroupKeySet groupKeySet, int group) {
        final GroupKey groupKey = groupKeySet.groupKey(group);
        if (groupKey instanceof InitialGroupKey) {
            return Collections.singletonList("");
        } else {
            final List<String> keys = Lists.newArrayList();
            int node = group;
            while (groupKeySet != null && groupKeySet.previous() != null) {
                groupKeySet.groupKey(node).addToList(keys);
                node = groupKeySet.parentGroup(node);
                groupKeySet = groupKeySet.previous();
            }
            return Lists.reverse(keys);
        }
    }
}
