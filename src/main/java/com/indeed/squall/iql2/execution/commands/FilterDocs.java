package com.indeed.squall.iql2.execution.commands;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.SessionCallback;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.util.core.TreeTimer;

import java.util.List;
import java.util.Map;

public class FilterDocs implements Command {
    public final Map<String, List<String>> perDatasetFilterMetric;

    public FilterDocs(Map<String, List<String>> perDatasetFilterMetric) {
        final Map<String, List<String>> copy = Maps.newHashMap();
        for (final Map.Entry<String, List<String>> entry : perDatasetFilterMetric.entrySet()) {
            copy.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.perDatasetFilterMetric = copy;
    }

    @Override
    public void execute(Session s, Consumer<String> out) throws ImhotepOutOfMemoryException {
        s.process(new SessionCallback() {
            @Override
            public void handle(TreeTimer timer, String name, ImhotepSession session) throws ImhotepOutOfMemoryException {
                if (!perDatasetFilterMetric.containsKey(name)) {
                    return;
                }

                timer.push("pushStats");
                final int index = session.pushStats(perDatasetFilterMetric.get(name));
                timer.pop();

                if (index != 1) {
                    throw new IllegalArgumentException("Didn't end up with 1 stat after pushing in index named \"" + name + "\"");
                }

                timer.push("metricFilter");
                session.metricFilter(0, 1, 1, false);
                timer.pop();

                timer.push("popStat");
                session.popStat();
                timer.pop();
            }
        });
        out.accept("{}");
    }
}
