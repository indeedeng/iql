package com.indeed.squall.iql2.execution.groupkeys;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class TimeRangeGroupKey extends GroupKey {
    private final String format;
    private final long start;
    private final long end;
    private final DateTimeFormatter formatter;

    public TimeRangeGroupKey(String format, long start, long end) {
        this.format = format;
        this.start = start;
        this.end = end;
        formatter = DateTimeFormat.forPattern(format).withLocale(Locale.US);
    }

    @Override
    public void addToList(List<String> list) {
        list.add("[" + formatter.print(start) + ", " + formatter.print(end) + ")");
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
