package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.StringGroupKey;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.util.Objects;

public class YearMonthGroupKey implements GroupKeySet {
    private final GroupKeySet previous;
    private final int numMonths;
    private final DateTime startMonth;
    private final String formatString;
    private final DateTimeFormatter formatter;

    public YearMonthGroupKey(GroupKeySet previous, int numMonths, DateTime startMonth, String formatString) {
        this.previous = previous;
        this.numMonths = numMonths;
        this.startMonth = startMonth;
        this.formatString = formatString;
        this.formatter = DateTimeFormat.forPattern(formatString).withLocale(Locale.US);
    }

    @Override
    public GroupKeySet previous() {
        return previous;
    }

    @Override
    public int parentGroup(int group) {
        return 1 + (group - 1) / numMonths;
    }

    @Override
    public GroupKey groupKey(int group) {
        final int monthOffset = (group - 1) % numMonths;
        final DateTime month = startMonth.plusMonths(monthOffset);
        return new StringGroupKey(formatter.print(month));
    }

    @Override
    public int numGroups() {
        return previous.numGroups() * numMonths;
    }

    @Override
    public boolean isPresent(int group) {
        return group > 0 && group <= numGroups() && previous.isPresent(parentGroup(group));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        YearMonthGroupKey that = (YearMonthGroupKey) o;
        return numMonths == that.numMonths &&
                Objects.equals(previous, that.previous) &&
                Objects.equals(startMonth, that.startMonth) &&
                Objects.equals(formatString, that.formatString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(previous, numMonths, startMonth, formatString);
    }
}
