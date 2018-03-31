package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.InitialGroupKey;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
        return group > 0 && group < groupParents.length && groupKeys.get(group) != null && (previous == null || previous.isPresent(parentGroup(group)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DumbGroupKeySet that = (DumbGroupKeySet) o;
        return Objects.equals(previous, that.previous) &&
                Arrays.equals(groupParents, that.groupParents) &&
                Objects.equals(groupKeys, that.groupKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(previous, groupParents, groupKeys);
    }
}
