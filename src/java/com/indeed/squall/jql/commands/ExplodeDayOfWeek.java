package com.indeed.squall.jql.commands;

import com.google.common.collect.Lists;
import com.indeed.common.util.Pair;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Session;
import com.indeed.squall.jql.TimeUnit;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Optional;

public class ExplodeDayOfWeek {
    public static void explodeDayOfWeek(Session session) throws ImhotepOutOfMemoryException {
        final String[] dayKeys = { "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday" };

        final long start = new DateTime(session.getEarliestStart()).withTimeAtStartOfDay().getMillis();
        final long end = new DateTime(session.getLatestEnd()).plusDays(1).withTimeAtStartOfDay().getMillis();
        final int numGroups = session.performTimeRegroup(start, end, TimeUnit.DAY.millis, Optional.empty());
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
        final int oldNumGroups = session.numGroups;
        session.sessions.values().forEach(sessionInfo -> Session.unchecked(() -> sessionInfo.session.regroup(rulesArray)));
        session.assumeDense(group -> {
            final int originalGroup = 1 + (group - 1) / dayKeys.length;
            final int dayOfWeek = (group - 1) % dayKeys.length;
            return Pair.of(dayKeys[dayOfWeek], session.groupKeys.get(originalGroup));
        }, oldNumGroups * dayKeys.length);
        session.currentDepth += 1;
    }
}
