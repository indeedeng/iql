package com.indeed.squall.iql2.execution.commands;

import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.AggregateFilter;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.SessionCallback;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.GroupKeySet;
import com.indeed.util.core.TreeTimer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ApplyGroupFilter implements Command {
    private final AggregateFilter filter;

    public ApplyGroupFilter(AggregateFilter filter) {
        this.filter = filter;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final Set<QualifiedPush> requires = filter.requires();
        final HashMap<QualifiedPush, Integer> metricIndexes = new HashMap<>();
        final HashMap<String, IntList> sessionMetricIndexes = new HashMap<>();
        session.pushMetrics(requires, metricIndexes, sessionMetricIndexes);
        filter.register(metricIndexes, session.groupKeySet);
        final long[][] stats = new long[metricIndexes.size()][];
        session.process(new SessionCallback() {
            @Override
            public void handle(TreeTimer timer, String name, ImhotepSession session) throws ImhotepOutOfMemoryException {
                int index = -1;
                for (final Map.Entry<QualifiedPush, Integer> entry : metricIndexes.entrySet()) {
                    index += 1;
                    if (!entry.getKey().sessionName.equals(name)) {
                        continue;
                    }
                    final List<String> pushes = entry.getKey().pushes;
                    session.pushStats(pushes);
                    final long[] groupStats = session.getGroupStats(0);
                    session.popStat();
                    synchronized (stats) {
                        stats[index] = groupStats;
                    }
                }
            }
        });
        final boolean[] keep = filter.getGroupStats(stats, session.numGroups);
        final List<GroupRemapRule> rules = new ArrayList<>();
        final RegroupCondition fakeCondition = new RegroupCondition("foo", true, 0, null, false);
        final List<GroupKey> newGroupKeys = new ArrayList<>();
        newGroupKeys.add(null);
        final IntList newGroupParents = new IntArrayList();
        newGroupParents.add(-1);
        for (int i = 1; i < rules.size(); i++) {
            final int newGroup = keep[i] ? i : 0;
            rules.add(new GroupRemapRule(i, fakeCondition, newGroup, newGroup));
            if (keep[i]) {
                newGroupKeys.add(session.groupKeySet.groupKeys.get(i));
                newGroupParents.add(i);
            }
        }
        final GroupRemapRule[] rulesArray = rules.toArray(new GroupRemapRule[rules.size()]);
        session.process(new SessionCallback() {
            @Override
            public void handle(TreeTimer timer, String name, ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.regroup(rulesArray);
            }
        });
        session.groupKeySet = GroupKeySet.create(session.groupKeySet, newGroupParents.toIntArray(), newGroupKeys);
        out.accept("done");
    }
}
