package com.indeed.squall.iql2.execution.groupkeys;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DumbGroupKeySet implements GroupKeySet {
    public final GroupKeySet previous;
    public final int[] groupParents;
    public final List<GroupKey> groupKeys;

    private DumbGroupKeySet(GroupKeySet previous, int[] groupParents, List<GroupKey> groupKeys) {
        this.previous = previous;
        this.groupParents = groupParents;
        this.groupKeys = Collections.unmodifiableList(groupKeys);
    }

    public static DumbGroupKeySet create() {
        return new DumbGroupKeySet(null, new int[]{-1, -1}, Arrays.<GroupKey>asList(null, InitialGroupKey.INSTANCE));
    }

    public static DumbGroupKeySet create(GroupKeySet previous, int[] groupParents, List<GroupKey> groupKeys) {
        return new DumbGroupKeySet(previous, groupParents, groupKeys);
    }

    @Override
    public List<String> asList(int group) {
        final GroupKey groupKey = groupKeys.get(group);
        if (groupKey instanceof InitialGroupKey) {
            return Collections.singletonList("");
        } else {
            final List<String> keys = Lists.newArrayList();
            GroupKeySet groupKeySet = this;
            int node = group;
            while (groupKeySet != null && groupKeySet.previous() != null) {
                groupKeySet.groupKey(node).addToList(keys);
                node = groupKeySet.parentGroup(node);
                groupKeySet = groupKeySet.previous();
            }
            return Lists.reverse(keys);
        }
    }

    @Override
    public GroupKeySet previous() {
        return previous;
    }

    @Override
    public int parentGroup(int group) {
        return groupParents[group];
    }

    @Override
    public GroupKey groupKey(int group) {
        return groupKeys.get(group);
    }

    @Override
    public int numGroups() {
        return groupKeys.size();
    }

    @Override
    public boolean isPresent(int group) {
        return group < groupParents.length && groupParents[group] != -1;
    }
}
