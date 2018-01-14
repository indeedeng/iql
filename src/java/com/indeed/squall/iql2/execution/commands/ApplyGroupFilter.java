package com.indeed.squall.iql2.execution.commands;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.squall.iql2.execution.AggregateFilter;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.SessionCallback;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import com.indeed.util.core.TreeTimer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ApplyGroupFilter implements Command {
    private static final Logger log = Logger.getLogger(ApplyGroupFilter.class);

    private final AggregateFilter filter;

    public ApplyGroupFilter(AggregateFilter filter) {
        this.filter = filter;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final Set<QualifiedPush> requires = filter.requires();
        final HashMap<QualifiedPush, Integer> metricIndexes = new HashMap<>();
        final HashMap<String, IntList> sessionMetricIndexes = new HashMap<>();
        session.pushMetrics(requires, metricIndexes, sessionMetricIndexes, true);
        filter.register(metricIndexes, session.groupKeySet);
        final long[][] stats = new long[metricIndexes.size()][];
        session.process(new SessionCallback() {
            @Override
            public void handle(TreeTimer timer, String name, ImhotepSession session) throws ImhotepOutOfMemoryException {
                for (final Map.Entry<QualifiedPush, Integer> entry : metricIndexes.entrySet()) {
                    if (!entry.getKey().sessionName.equals(name)) {
                        continue;
                    }
                    final List<String> pushes = entry.getKey().pushes;
                    session.pushStats(pushes);
                    final long[] groupStats = session.getGroupStats(0);
                    session.popStat();
                    synchronized (stats) {
                        stats[entry.getValue()] = groupStats;
                    }
                }
            }
        });
        final boolean[] keep = filter.getGroupStats(stats, session.numGroups);
        List<GroupMultiRemapMessage> rules = Lists.newArrayList();
        final List<RegroupConditionMessage> fakeConditions = Lists.newArrayList(RegroupConditionMessage.newBuilder()
                .setField("fakeField")
                .setIntType(true)
                .setIntTerm(0)
                .setInequality(false)
                .build());
        final List<GroupKey> newGroupKeys = new ArrayList<>();
        newGroupKeys.add(null);
        final IntList newGroupParents = new IntArrayList();
        newGroupParents.add(-1);
        for (int i = 1; i < keep.length; i++) {
            final int newGroup = keep[i] ? newGroupParents.size() : 0;
            rules.add(GroupMultiRemapMessage.newBuilder()
                    .setTargetGroup(i)
                    .setNegativeGroup(newGroup)
                    .addAllPositiveGroup(Ints.asList(new int[] {newGroup}))
                    .addAllCondition(fakeConditions)
                    .build());
            if (keep[i]) {
                newGroupKeys.add(session.groupKeySet.groupKey(i));
                newGroupParents.add(session.groupKeySet.parentGroup(i));
            }
        }
        final GroupMultiRemapMessage[] messages = rules.toArray(new GroupMultiRemapMessage[0]);
        session.process(new SessionCallback() {
            @Override
            public void handle(TreeTimer timer, String name, ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.regroupWithProtos(messages, true);
            }
        });
        session.groupKeySet = DumbGroupKeySet.create(session.groupKeySet.previous(), newGroupParents.toIntArray(), newGroupKeys);
        session.numGroups = session.groupKeySet.numGroups();
        out.accept("done");
    }
}
