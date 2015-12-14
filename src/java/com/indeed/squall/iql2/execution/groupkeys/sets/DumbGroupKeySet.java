package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.InitialGroupKey;

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
        return groupKeys.size() - 1;
    }

    @Override
    public boolean isPresent(int group) {
        return group < groupParents.length && groupParents[group] != -1 && (previous == null || previous.isPresent(parentGroup(group)));
    }
}
