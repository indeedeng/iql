package com.indeed.squall.iql2.execution.groupkeys;

import com.indeed.squall.iql2.execution.commands.ExplodeDayOfWeek;

import java.util.List;
import java.util.Objects;

public class DayOfWeekGroupKey extends GroupKey {
    private final int dayOfWeek;

    public DayOfWeekGroupKey(int dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
    }

    @Override
    public void addToList(List<String> list) {
        list.add(ExplodeDayOfWeek.DAY_KEYS[dayOfWeek]);
    }

    @Override
    public boolean isDefault() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DayOfWeekGroupKey that = (DayOfWeekGroupKey) o;
        return dayOfWeek == that.dayOfWeek;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dayOfWeek);
    }

    @Override
    public String toString() {
        return "DayOfWeekGroupKey{" +
                "dayOfWeek=" + dayOfWeek +
                '}';
    }
}
