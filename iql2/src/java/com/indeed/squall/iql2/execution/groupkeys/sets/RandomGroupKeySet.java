package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.groupkeys.DefaultGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.IntTermGroupKey;

public class RandomGroupKeySet implements GroupKeySet {
    private final GroupKeySet previous;
    private final int numGroups;

    public RandomGroupKeySet(GroupKeySet previous, int numGroups) {
        this.previous = previous;
        this.numGroups = numGroups;
    }

    @Override
    public GroupKeySet previous() {
        return previous;
    }

    @Override
    public int parentGroup(int group) {
        return 1;
    }

    @Override
    public GroupKey groupKey(int group) {
        if (group == 1) {
            return DefaultGroupKey.create("No term");
        }
        return new IntTermGroupKey(group - 1);
    }

    @Override
    public int numGroups() {
        return numGroups;
    }

    @Override
    public boolean isPresent(int group) {
        return group <= numGroups;
    }
}
