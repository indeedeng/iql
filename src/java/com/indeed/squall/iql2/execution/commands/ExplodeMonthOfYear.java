package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.TimeUnit;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.sets.YearMonthGroupKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;

import java.util.List;

public class ExplodeMonthOfYear implements Command {
    @Override
    public void execute(final Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        final long earliestStart = session.getEarliestStart();
        final long latestEnd = session.getLatestEnd();
        final TimeUnit timeUnit = TimeUnit.MONTH;

        final long unitSize = TimeUnit.DAY.millis;
        final long timeOffsetMinutes = 0L;
        final DateTimeZone zone = DateTimeZone.forOffsetHoursMinutes((int) timeOffsetMinutes / 60, (int) timeOffsetMinutes % 60);
        final long realStart = new DateTime(earliestStart, zone).getMillis();
        final long shardsEnd = new DateTime(latestEnd, zone).getMillis();
        final long difference = shardsEnd - realStart;
        final long realEnd;
        if (difference % timeUnit.millis == 0) {
            realEnd = shardsEnd;
        } else {
            realEnd = shardsEnd + (timeUnit.millis - difference % timeUnit.millis);
        }

        final int oldNumGroups = session.numGroups;

        final int numBuckets = (int)Math.ceil(((double)realEnd - realStart) / unitSize);
        final DateTime startMonth = new DateTime(earliestStart, zone).withDayOfMonth(1).withTimeAtStartOfDay();
        final DateTime endMonthExclusive = new DateTime(latestEnd, zone).minusDays(1).withDayOfMonth(1).withTimeAtStartOfDay().plusMonths(1);
        final int numMonths = Months.monthsBetween(
                startMonth,
                endMonthExclusive
        ).getMonths();
        session.checkGroupLimit(numMonths * session.numGroups);

        final int numGroups = session.performTimeRegroup(realStart, realEnd, unitSize, Optional.<String>absent(), false);
        session.checkGroupLimit(numGroups);

        session.timer.push("compute month remapping");
        final List<GroupRemapRule> rules = Lists.newArrayList();
        final RegroupCondition fakeCondition = new RegroupCondition("fakeField", true, 0, null, false);
        for (int outerGroup = 1; outerGroup <= oldNumGroups; outerGroup++) {
            for (int innerGroup = 0; innerGroup < numBuckets; innerGroup++) {
                final long start = realStart + innerGroup * unitSize;
                final int base = 1 + (outerGroup - 1) * numBuckets + innerGroup;
                final int newBase = 1 + (outerGroup - 1) * numMonths;

                final DateTime date = new DateTime(start, zone).withDayOfMonth(1).withTimeAtStartOfDay();
                final int newGroup = newBase + Months.monthsBetween(startMonth, date).getMonths();
                rules.add(new GroupRemapRule(base, fakeCondition, newGroup, newGroup));
            }
        }
        final GroupRemapRule[] rulesArray = rules.toArray(new GroupRemapRule[0]);
        session.timer.pop();

        session.regroup(rulesArray);

        session.assumeDense(new YearMonthGroupKey(session.groupKeySet, numMonths, startMonth, TimeUnit.MONTH.formatString));
    }
}
