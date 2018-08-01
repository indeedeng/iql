/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.execution.commands;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.SessionCallback;
import com.indeed.iql2.execution.compat.Consumer;
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

                final int index = Session.pushStatsWithTimer(session, perDatasetFilterMetric.get(name), timer);

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
