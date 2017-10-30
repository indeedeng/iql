package com.indeed.squall.iql2.execution.commands;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.SessionCallback;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import com.indeed.util.core.TreeTimer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExplodePerDocPercentile implements Command {
    public final String field;
    public final int numBuckets;

    public ExplodePerDocPercentile(String field, int numBuckets) {
        this.field = field;
        this.numBuckets = numBuckets;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final String field = this.field;
        final int numBuckets = this.numBuckets;

        session.checkGroupLimit(numBuckets * session.numGroups);

        session.timer.push("get counts");
        final long[] counts = new long[session.numGroups + 1];
        for (final Session.ImhotepSessionInfo s : session.sessions.values()) {
            s.session.pushStat("count()");
            final long[] stats = s.session.getGroupStats(0);
            for (int i = 0; i < stats.length; i++) {
                counts[i] += stats[i];
            }
        }
        session.timer.pop();

        final long[] runningCounts = new long[session.numGroups + 1];
        final long[][] cutoffs = new long[session.numGroups + 1][numBuckets];
        final int[] soFar = new int[session.numGroups + 1];
        final Map<String, IntList> metricIndexes = Maps.newHashMap();
        for (final String k : session.sessions.keySet()) {
            metricIndexes.put(k, new IntArrayList(new int[]{0}));
        }
        session.timer.push("compute cutoffs (iterateMultiInt)");
        session.iterateMultiInt(session.getSessionsMapRaw(), metricIndexes, Collections.<String, Integer>emptyMap(), field, new Session.IntIterateCallback() {
            @Override
            public void term(long term, long[] stats, int group) {
                runningCounts[group] += stats[0];
                final int fraction = (int) Math.floor((double) numBuckets * runningCounts[group] / counts[group]);
                for (int i = soFar[group] + 1; i <= fraction; i++) {
                    cutoffs[group][i - 1] = term;
                    soFar[group] = i;
                }
            }
        }, session.timer);
        session.timer.pop();

        for (int group = 1; group <= session.numGroups; group++) {
            for (int idx = soFar[group] + 1; idx < numBuckets; idx++) {
                cutoffs[group][idx] = Integer.MAX_VALUE;
            }
        }

        session.timer.push("compute bucket remaps");
        final List<GroupMultiRemapMessage> rules = Lists.newArrayList();
        final List<GroupKey> nextGroupKeys = Lists.newArrayList();
        final IntList groupParents = new IntArrayList();
        nextGroupKeys.add(null);
        groupParents.add(-1);
        final RegroupConditionMessage.Builder conditionBuilder = RegroupConditionMessage.newBuilder()
                .setField(field)
                .setIntType(true)
                .setInequality(true);
        for (int group = 1; group <= session.numGroups; group++) {
            final IntArrayList positiveGroups = new IntArrayList();
            final List<RegroupConditionMessage> conditions = Lists.newArrayList();
            for (int bucket = 0; bucket < numBuckets; bucket++) {
                if (bucket > 0 && cutoffs[group][bucket] == cutoffs[group][bucket - 1]) {
                    continue;
                }
                final int end = ArrayUtils.lastIndexOf(cutoffs[group], cutoffs[group][bucket]);
                final String keyTerm = "[" + (double) bucket / numBuckets + ", " + (double) (end + 1) / numBuckets + ")";
                final int newGroup = nextGroupKeys.size();
                // TODO: Not use StringGroupKey this.
                nextGroupKeys.add(new StringGroupKey(keyTerm));
                groupParents.add(group);
                positiveGroups.add(newGroup);
                conditions.add(conditionBuilder.setIntTerm(cutoffs[group][bucket]).build());
            }
            rules.add(GroupMultiRemapMessage.newBuilder()
                    .setTargetGroup(group)
                    .setNegativeGroup(0)
                    .addAllPositiveGroup(positiveGroups)
                    .addAllCondition(conditions)
                    .build());
        }

        final GroupMultiRemapMessage[] messages = rules.toArray(new GroupMultiRemapMessage[rules.size()]);
        session.timer.pop();

        session.process(new SessionCallback() {
            @Override
            public void handle(TreeTimer timer, String name, ImhotepSession session) throws ImhotepOutOfMemoryException {
                timer.push("regroup");
                session.regroupWithProtos(messages, true);
                timer.pop();

                timer.push("popStat");
                session.popStat();
                timer.pop();
            }
        });

        session.assumeDense(DumbGroupKeySet.create(session.groupKeySet, groupParents.toIntArray(), nextGroupKeys));

        out.accept("ExplodedPerDocPercentile");
    }
}
