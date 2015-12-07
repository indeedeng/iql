package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.TimeUnit;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.DumbGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.GroupKeyCreator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        session.performTimeRegroup(realStart, realEnd, unitSize, Optional.<String>absent());
        final int numBuckets = (int)Math.ceil(((double)realEnd - realStart) / unitSize);
        final DateTimeFormatter formatter = DateTimeFormat.forPattern(TimeUnit.MONTH.formatString);
        final DateTime startMonth = new DateTime(earliestStart, zone).withDayOfMonth(1).withTimeAtStartOfDay();
        final DateTime endMonthExclusive = new DateTime(latestEnd, zone).minusDays(1).withDayOfMonth(1).withTimeAtStartOfDay().plusMonths(1);
        final int numMonths = Months.monthsBetween(
                startMonth,
                endMonthExclusive
        ).getMonths();

        session.timer.push("compute month remapping");
        final List<GroupRemapRule> rules = Lists.newArrayList();
        final RegroupCondition fakeCondition = new RegroupCondition("fakeField", true, 100, null, false);
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
        final GroupRemapRule[] rulesArray = rules.toArray(new GroupRemapRule[rules.size()]);
        session.timer.pop();

        session.regroup(rulesArray);

        final Map<DateTime, GroupKey> yearMonthToGroupKey = new HashMap<>();

        final GroupKeyCreator groupMapper = new GroupKeyCreator() {
            @Override
            public int parent(int group) {
                return 1 + (group - 1) / numMonths;
            }

            @Override
            public GroupKey forIndex(int group) {
                final int monthOffset = (group - 1) % numMonths;
                final DateTime month = startMonth.plusMonths(monthOffset);
                if (!yearMonthToGroupKey.containsKey(month)) {
                    yearMonthToGroupKey.put(month, new DumbGroupKey(formatter.print(month)));
                }
                return yearMonthToGroupKey.get(month);
            }
        };
        if (oldNumGroups == 1) {
            session.assumeDense(groupMapper, oldNumGroups * numMonths);
        } else {
            session.densify(groupMapper);
        }
        session.currentDepth += 1;
    }
}
