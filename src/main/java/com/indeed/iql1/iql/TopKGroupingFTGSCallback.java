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
 package com.indeed.iql1.iql;

import com.google.common.collect.Lists;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.web.Limits;
import com.indeed.iql1.ez.EZImhotepSession;
import com.indeed.iql1.ez.GroupKey;
import com.indeed.iql1.ez.StatReference;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * @author jplaisance
 */
public final class TopKGroupingFTGSCallback extends EZImhotepSession.FTGSCallback {
    private final Int2ObjectMap<PriorityQueue<ScoredObject<GroupStats>>> groupToTopK = new Int2ObjectOpenHashMap<>();
    private final Comparator<ScoredObject<GroupStats>> comparator;
    private final Limits limits;
    private final int topK;
    private final boolean isBottom;
    private final StatReference countStat;
    private final List<StatReference> statRefs;
    private final Int2ObjectMap<GroupKey> groupKeys;
    private int newGroupCount = 0;

    public TopKGroupingFTGSCallback(final int numStats, int topK, StatReference countStat, List<StatReference> statRefs,
                                    Int2ObjectMap<GroupKey> groupKeys, boolean isBottom, Limits limits) {
        super(numStats);
        this.topK = topK;
        this.isBottom = isBottom;
        this.countStat = countStat;
        this.statRefs = statRefs;
        this.groupKeys = groupKeys;

        this.comparator = isBottom ? ScoredObject.bottomScoredObjectComparator(GroupStats.GROUP_STATS_COMPARATOR) : ScoredObject.topScoredObjectComparator(GroupStats.GROUP_STATS_COMPARATOR);
        this.limits = limits;
    }

    protected void intTermGroup(final long term, final int group) {
        termGroup(term, group);
    }

    protected void stringTermGroup(final String term, final int group) {
        termGroup(term, group);
    }

    private void termGroup(final Comparable term, final int group) {
        PriorityQueue<ScoredObject<GroupStats>> topTerms = groupToTopK.get(group);
        if (topTerms == null) {
            topTerms = new PriorityQueue<>(comparator);
            groupToTopK.put(group, topTerms);
        }
        final double count = getStat(countStat);
        if (topTerms.size() < topK) {
            topTerms.add(getStats(count, group, term));

            if(!limits.satisfiesQueryInMemoryRowsLimit(++newGroupCount)) {
                throw new IqlKnownException.GroupLimitExceededException("Number of groups exceeds the limit " +
                        new DecimalFormat("###,###").format(limits.queryInMemoryRowsLimit) +
                        ". Please simplify the query.");
            }
        } else {
            final double headCount = topTerms.peek().getScore();
            if ((!isBottom && count > headCount) ||
                    (isBottom && count < headCount) ||
                    (Double.isNaN(headCount) && !Double.isNaN(count))) {
                topTerms.remove();
                topTerms.add(getStats(count, group, term));
            }
        }
    }

    private ScoredObject<GroupStats> getStats(double count, int group, Comparable term) {
        final double[] stats = new double[statRefs.size()];
        for (int i = 0; i < statRefs.size(); i++) {
            stats[i] = getStat(statRefs.get(i));
        }
        return new ScoredObject<>(count, new GroupStats(groupKeys.get(group).add(term), stats));
    }

    public List<GroupStats> getResults() {
        final List<GroupStats> ret = Lists.newArrayList();
        final GroupStats[] buffer = new GroupStats[topK];
        for (int group = 1; group <= groupKeys.size(); group++) {
            final PriorityQueue<ScoredObject<GroupStats>> topTerms = groupToTopK.get(group);
            if (topTerms != null) {
                final int numTopTerms = topTerms.size();
                for (int i = numTopTerms - 1; i >= 0; i--) {
                    buffer[i] = topTerms.remove().getObject();
                }
                ret.addAll(Arrays.asList(buffer).subList(0, numTopTerms));
            } else {    // TODO: do we want these empty rows?
                ret.add(new GroupStats(groupKeys.get(group).add(""), new double[statRefs.size()]));
            }
        }
        return ret;
    }
}
