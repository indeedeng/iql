package com.indeed.squall.iql2.execution.groupkeys;

import org.joda.time.DateTime;

import java.util.List;
import java.util.Objects;

public class TimeRangeGroupKey extends GroupKey {
    private final String format;
    private final long start;
    private final long end;

    public TimeRangeGroupKey(String format, long start, long end) {
        this.format = format;
        this.start = start;
        this.end = end;
    }

    @Override
    public void addToList(List<String> list) {
        list.add("[" + new DateTime(start).toString(format) + ", " + new DateTime(end).toString(format) + ")");
    }

    @Override
    public boolean isDefault() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeRangeGroupKey that = (TimeRangeGroupKey) o;
        return start == that.start &&
                end == that.end &&
                Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, start, end);
    }

    @Override
    public String toString() {
        return "TimeRangeGroupKey{" +
                "format='" + format + '\'' +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
