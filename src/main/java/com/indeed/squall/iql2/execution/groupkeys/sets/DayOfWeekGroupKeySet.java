package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.commands.ExplodeDayOfWeek;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;

import java.util.Objects;

public class DayOfWeekGroupKeySet implements GroupKeySet {
    private final GroupKeySet previous;

    public DayOfWeekGroupKeySet(GroupKeySet previous) {
        this.previous = previous;
    }

    @Override
    public GroupKeySet previous() {
        return previous;
    }

    @Override
    public int parentGroup(int group) {
        return 1 + (group - 1) / ExplodeDayOfWeek.DAY_KEYS.length;
    }

    @Override
    public GroupKey groupKey(int group) {
        final int dayOfWeek = (group - 1) % ExplodeDayOfWeek.DAY_KEYS.length;
        return ExplodeDayOfWeek.DAY_GROUP_KEYS[dayOfWeek];
    }

    @Override
    public int numGroups() {
        return ExplodeDayOfWeek.DAY_KEYS.length * previous.numGroups();
    }

    @Override
    public boolean isPresent(int group) {
        return group > 0 && group <= numGroups() && previous.isPresent(parentGroup(group));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DayOfWeekGroupKeySet that = (DayOfWeekGroupKeySet) o;
        return Objects.equals(previous, that.previous);
    }

    @Override
    public int hashCode() {
        return Objects.hash(previous);
    }
}
