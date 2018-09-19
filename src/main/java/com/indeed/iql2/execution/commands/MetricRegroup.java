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
import com.google.common.collect.ImmutableMap;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.SessionCallback;
import java.util.function.Consumer;
import com.indeed.iql2.execution.groupkeys.sets.MetricRangeGroupKeySet;
import com.indeed.util.core.TreeTimer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MetricRegroup implements Command {
    public final ImmutableMap<String, ImmutableList<String>> perDatasetMetric;
    public final long min;
    public final long max;
    public final long interval;
    public final boolean excludeGutters;
    public final boolean withDefault;
    public final boolean fromPredicate;

    public MetricRegroup(Map<String, ? extends List<String>> perDatasetMetric, long min, long max, long interval, boolean excludeGutters, boolean withDefault, boolean fromPredicate) {
        final ImmutableMap.Builder<String, ImmutableList<String>> copy = ImmutableMap.builder();
        for (final Map.Entry<String, ? extends List<String>> entry : perDatasetMetric.entrySet()) {
            copy.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.perDatasetMetric = copy.build();
        this.min = min;
        this.max = max;
        this.interval = interval;
        this.excludeGutters = excludeGutters;
        this.withDefault = withDefault;
        this.fromPredicate = fromPredicate;
    }

    @Override
    public void execute(final Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        final long max = this.max;
        final long min = this.min;
        final long interval = this.interval;
        final Map<String, ? extends List<String>> perDatasetMetrics = this.perDatasetMetric;

        final boolean withDefaultBucket = withDefault && excludeGutters;

        final int intermediateBuckets = ((excludeGutters && !withDefaultBucket) ? 0 : 2) + (int) Math.ceil(((double) max - min) / interval);

        final int groupsBefore = session.numGroups;
        session.checkGroupLimit(intermediateBuckets * groupsBefore);

        session.process(new SessionCallback() {
            @Override
            public void handle(TreeTimer timer, String name, ImhotepSessionHolder session) throws ImhotepOutOfMemoryException {
                if (!perDatasetMetrics.containsKey(name)) {
                    return;
                }
                final List<String> pushes = new ArrayList<>(perDatasetMetrics.get(name));

                final int numStats = Session.pushStatsWithTimer(session, pushes, timer);

                if (numStats != 1) {
                    throw new IllegalStateException("Pushed more than one stat!: " + pushes);
                }

                timer.push("metricRegroup");
                session.metricRegroup(0, min, max, interval, excludeGutters && !withDefaultBucket);
                timer.pop();

                if (withDefaultBucket) {
                    timer.push("merge gutters into default/regroupWithProtos(" + intermediateBuckets * groupsBefore + " rules)" );
                    final GroupMultiRemapMessage[] rules = new GroupMultiRemapMessage[intermediateBuckets * groupsBefore];
                    for (int i = 0; i < rules.length; i++) {
                        final int group = i + 1;
                        final int groupOffset = (group - 1) % intermediateBuckets;
                        final int prevGroup = 1 + (group - 1) / intermediateBuckets;
                        final int newGroup;
                        if (groupOffset == intermediateBuckets - 1 || groupOffset == intermediateBuckets - 2) {
                            newGroup = 1 + (prevGroup - 1) * (intermediateBuckets - 1) + (intermediateBuckets - 2);
                        } else {
                            newGroup = 1 + (prevGroup - 1) * (intermediateBuckets - 1) + groupOffset;
                        }
                        rules[i] = GroupMultiRemapMessage.newBuilder()
                                .setTargetGroup(group)
                                .setNegativeGroup(newGroup)
                                .build();
                    }
                    session.remapGroups(rules);
                    timer.pop();
                }

                timer.push("popStat");
                session.popStat();
                timer.pop();
            }
        });

        session.assumeDense(new MetricRangeGroupKeySet(session.groupKeySet, withDefaultBucket ? intermediateBuckets - 1 : intermediateBuckets, excludeGutters, min, interval, withDefaultBucket, fromPredicate));

        out.accept("success");
    }
}
