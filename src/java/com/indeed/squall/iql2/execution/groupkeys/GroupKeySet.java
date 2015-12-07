package com.indeed.squall.iql2.execution.groupkeys;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GroupKeySet {
    public final GroupKeySet previous;
    public final int[] groupParents;
    public final List<GroupKey> groupKeys;

    private GroupKeySet(GroupKeySet previous, int[] groupParents, List<GroupKey> groupKeys) {
        this.previous = previous;
        this.groupParents = groupParents;
        this.groupKeys = Collections.unmodifiableList(groupKeys);
    }

    public static GroupKeySet create() {
        return new GroupKeySet(null, new int[]{-1, -1}, Arrays.<GroupKey>asList(null, InitialGroupKey.INSTANCE));
    }

    public static GroupKeySet create(GroupKeySet previous, int[] groupParents, List<GroupKey> groupKeys) {
        return new GroupKeySet(previous, groupParents, groupKeys);
    }

    public List<String> asList(int group, boolean appendingTerm) {
        final GroupKey groupKey = groupKeys.get(group);
        if (groupKey instanceof InitialGroupKey && !appendingTerm) {
            return Collections.singletonList("");
        } else {
            final List<String> keys = Lists.newArrayList();
            GroupKeySet groupKeySet = this;
            int node = group;
            while (groupKeySet != null && groupKeySet.groupParents != null) {
                groupKeySet.groupKeys.get(node).addToList(keys, appendingTerm);
                node = groupKeySet.groupParents[node];
                groupKeySet = groupKeySet.previous;
            }
            return Lists.reverse(keys);
        }
    }
}
