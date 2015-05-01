package com.indeed.squall.jql.commands;

import com.google.common.collect.Lists;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Session;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class ExplodePerDocPercentile {
    public final String field;
    public final int numBuckets;

    public ExplodePerDocPercentile(String field, int numBuckets) {
        this.field = field;
        this.numBuckets = numBuckets;
    }

    public static void explodePerDocPercentile(ExplodePerDocPercentile explodeCommand, Session session) throws ImhotepOutOfMemoryException, IOException {
        final String field = explodeCommand.field;
        final int numBuckets = explodeCommand.numBuckets;

        final long[] counts = new long[session.numGroups + 1];
        session.sessions.values().forEach(s -> Session.unchecked(() -> {
            s.session.pushStat("count()");
            final long[] stats = s.session.getGroupStats(0);
            for (int i = 0; i < stats.length; i++) {
                counts[i] += stats[i];
            }
        }));

        final long[] runningCounts = new long[session.numGroups + 1];
        final long[][] cutoffs = new long[session.numGroups + 1][numBuckets];
        final int[] soFar = new int[session.numGroups + 1];
        Session.iterateMultiInt(session.getSessionsMapRaw(), session.sessions.keySet().stream().collect(Collectors.toMap(k -> k, k -> new IntArrayList(new int[]{0}))), field, new Session.IntIterateCallback() {
            @Override
            public void term(long term, long[] stats, int group) {
                runningCounts[group] += stats[0];
                final int fraction = (int) Math.floor((double) numBuckets * runningCounts[group] / counts[group]);
                for (int i = soFar[group] + 1; i < fraction; i++) {
                    cutoffs[group][i] = term;
                    soFar[group] = i;
                }
            }
        });

        for (int group = 1; group <= session.numGroups; group++) {
            for (int idx = soFar[group] + 1; idx < numBuckets; idx++) {
                cutoffs[group][idx] = Integer.MAX_VALUE;
            }
        }

        final List<GroupMultiRemapRule> rules = Lists.newArrayList();
        final List<Session.GroupKey> nextGroupKeys = Lists.newArrayList();
        nextGroupKeys.add(null);
        for (int group = 1; group <= session.numGroups; group++) {
            final IntArrayList positiveGroups = new IntArrayList();
            final List<RegroupCondition> conditions = Lists.newArrayList();
            for (int bucket = 0; bucket < numBuckets; bucket++) {
                if (bucket > 0 && cutoffs[group][bucket] == cutoffs[group][bucket - 1]) {
                    continue;
                }
                final int end = ArrayUtils.lastIndexOf(cutoffs[group], cutoffs[group][bucket]);
                final String keyTerm = "[" + (double) bucket / numBuckets + ", " + (double) (end + 1) / numBuckets + ")";
                final int newGroup = nextGroupKeys.size();
                nextGroupKeys.add(new Session.GroupKey(keyTerm, nextGroupKeys.size(), session.groupKeys.get(group)));
                positiveGroups.add(newGroup);
                conditions.add(new RegroupCondition(field, true, cutoffs[group][bucket], null, true));
            }
            final int[] positiveGroupsArr = positiveGroups.toIntArray(new int[positiveGroups.size()]);
            final RegroupCondition[] conditionsArr = conditions.toArray(new RegroupCondition[conditions.size()]);
            rules.add(new GroupMultiRemapRule(group, 0, positiveGroupsArr, conditionsArr));
        }

        final GroupMultiRemapRule[] rulesArr = rules.toArray(new GroupMultiRemapRule[rules.size()]);

        session.sessions.values().forEach(s -> Session.unchecked(() -> {
            s.session.regroup(rulesArr);
            s.session.popStat();
        }));

        session.numGroups = nextGroupKeys.size() - 1;
        session.groupKeys = nextGroupKeys;
        session.currentDepth += 1;
    }
}
