package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.groupkeys.GroupKey;

import java.util.BitSet;
import java.util.Objects;

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
        return group > 0 && group < presentMask.size() && presentMask.get(group);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaskingGroupKeySet that = (MaskingGroupKeySet) o;
        return Objects.equals(wrapped, that.wrapped) &&
                Objects.equals(presentMask, that.presentMask);
    }

    @Override
    public int hashCode() {
        return Objects.hash(wrapped, presentMask);
    }
}
