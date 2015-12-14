package com.indeed.squall.iql2.execution.groupkeys.sets;

import com.indeed.squall.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

public class YearMonthGroupKey implements GroupKeySet {
    private final GroupKeySet previous;
    private final int numMonths;
    private final DateTime startMonth;
    private final DateTimeFormatter formatter;

    public YearMonthGroupKey(GroupKeySet previous, int numMonths, DateTime startMonth, DateTimeFormatter formatter) {
        this.previous = previous;
        this.numMonths = numMonths;
        this.startMonth = startMonth;
        this.formatter = formatter;
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
        return group <= numGroups() && previous.isPresent(parentGroup(group));
    }
}
