package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.groupkeys.GroupKey;

import java.util.BitSet;

public class MaskingGroupKeySet implements GroupKeySet {
    private final GroupKeySet wrapped;
    private final BitSet presentMask;

    public MaskingGroupKeySet(GroupKeySet wrapped, BitSet presentMask) {
        this.wrapped = wrapped;
        this.presentMask = presentMask;
    }

    @Override
    public GroupKeySet previous() {
        return wrapped.previous();
    }

    @Override
    public int parentGroup(int group) {
        return wrapped.parentGroup(group);
    }

    @Override
    public GroupKey groupKey(int group) {
        return wrapped.groupKey(group);
    }

    @Override
    public int numGroups() {
        return wrapped.numGroups();
    }

    @Override
    public boolean isPresent(int group) {
        return group < presentMask.size() && presentMask.get(group);
    }
}
