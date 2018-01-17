package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.TimeUnit;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.sets.YearMonthGroupKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;

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
        final GroupMultiRemapMessage[] rules = new GroupMultiRemapMessage[oldNumGroups * numBuckets];
        final RegroupConditionMessage fakeCondition = RegroupConditionMessage.newBuilder()
                .setField("fakeField")
                .setIntType(true)
                .setIntTerm(0)
                .setInequality(false)
                .build();
        int index = 0;
        for (int outerGroup = 1; outerGroup <= oldNumGroups; outerGroup++) {
            for (int innerGroup = 0; innerGroup < numBuckets; innerGroup++) {
                final long start = realStart + innerGroup * unitSize;
                final int base = 1 + (outerGroup - 1) * numBuckets + innerGroup;
                final int newBase = 1 + (outerGroup - 1) * numMonths;

                final DateTime date = new DateTime(start, zone).withDayOfMonth(1).withTimeAtStartOfDay();
                final int newGroup = newBase + Months.monthsBetween(startMonth, date).getMonths();
                rules[index++] = GroupMultiRemapMessage.newBuilder()
                        .setTargetGroup(base)
                        .setNegativeGroup(newGroup)
                        .addCondition(fakeCondition)
                        .addPositiveGroup(newGroup)
                        .build();
            }
        }
        session.timer.pop();

        session.regroupWithProtos(rules, true);

        session.assumeDense(new YearMonthGroupKey(session.groupKeySet, numMonths, startMonth, TimeUnit.MONTH.formatString));
    }
}
