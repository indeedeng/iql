package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.indeed.common.util.Pair;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.TimeUnit;
import com.indeed.squall.iql2.execution.compat.Consumer;
import org.joda.time.DateTime;

import java.util.List;

public class ExplodeDayOfWeek implements Command {
    @Override
    public void execute(final Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        final String[] dayKeys = { "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday" };

        final long start = new DateTime(session.getEarliestStart()).withTimeAtStartOfDay().getMillis();
        final long end = new DateTime(session.getLatestEnd()).plusDays(1).withTimeAtStartOfDay().getMillis();
        session.timer.push("daily regroup");
        final int numGroups = session.performTimeRegroup(start, end, TimeUnit.DAY.millis, Optional.<String>absent());
        session.timer.pop();

        session.timer.push("compute remapping");
        final int numBuckets = (int) ((end - start) / TimeUnit.DAY.millis);
        final List<GroupRemapRule> rules = Lists.newArrayList();
        final RegroupCondition fakeCondition = new RegroupCondition("fakeField", true, 100, null, false);
        for (int group = 1; group <= numGroups; group++) {
            final int oldGroup = 1 + (group - 1) / numBuckets;
            final int dayOffset = (group - 1) % numBuckets;
            final long groupStart = start + dayOffset * TimeUnit.DAY.millis;
            final int newGroup = 1 + ((oldGroup - 1) * dayKeys.length) + new DateTime(groupStart).getDayOfWeek() - 1;
            rules.add(new GroupRemapRule(group, fakeCondition, newGroup, newGroup));
        }
        final GroupRemapRule[] rulesArray = rules.toArray(new GroupRemapRule[rules.size()]);
        session.timer.pop();
        final int oldNumGroups = session.numGroups;
        session.timer.push("shuffle regroup");
        session.regroup(rulesArray);
        session.timer.pop();
        session.assumeDense(new Function<Integer, Pair<String, Session.GroupKey>>() {
            public Pair<String, Session.GroupKey> apply(Integer group) {
                final int originalGroup = 1 + (group - 1) / dayKeys.length;
                final int dayOfWeek = (group - 1) % dayKeys.length;
                return Pair.of(dayKeys[dayOfWeek], session.groupKeys.get(originalGroup));
            }
        }, oldNumGroups * dayKeys.length);
        session.currentDepth += 1;

        out.accept("success");
    }
}
