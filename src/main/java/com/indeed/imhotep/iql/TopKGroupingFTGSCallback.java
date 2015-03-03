/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.iql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.util.core.Pair;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.GroupKey;
import com.indeed.imhotep.ez.StatReference;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.util.*;

/**
 * @author jplaisance
 */
public final class TopKGroupingFTGSCallback extends EZImhotepSession.FTGSCallback {
    private static final Logger log = Logger.getLogger(TopKGroupingFTGSCallback.class);

    private final Map<Integer, PriorityQueue<Pair<Double, GroupStats>>> groupToTopK = Maps.newHashMap();
    private final Comparator<Pair<Double, GroupStats>> comparator;
    private final int topK;
    private final boolean isBottom;
    private final StatReference countStat;
    private final List<StatReference> statRefs;
    private final Map<Integer, GroupKey> groupKeys;
    private int newGroupCount = 0;

    public TopKGroupingFTGSCallback(final int numStats, int topK, StatReference countStat, List<StatReference> statRefs,
                                    Map<Integer, GroupKey> groupKeys, boolean isBottom) {
        super(numStats);
        this.topK = topK;
        this.isBottom = isBottom;
        this.countStat = countStat;
        this.statRefs = statRefs;
        this.groupKeys = groupKeys;
        // do a custom comparator to ensure that real numbers are preferred to NaNs
        final Comparator<Pair<Double, GroupStats>> baseComparator = new Comparator<Pair<Double, GroupStats>>() {
            @Override
            public int compare(Pair<Double, GroupStats> o1, Pair<Double, GroupStats> o2) {
                Double a = o1.getFirst();
                if(a.isNaN()) {
                    a = Double.NEGATIVE_INFINITY;
                }
                Double b = o2.getFirst();
                if(b.isNaN()) {
                    b = Double.NEGATIVE_INFINITY;
                }
                return a.compareTo(b);
            }
        };
        final Comparator<Pair<Double, GroupStats>> reverseComparator = new Comparator<Pair<Double, GroupStats>>() {
            @Override
            public int compare(Pair<Double, GroupStats> o1, Pair<Double, GroupStats> o2) {
                Double a = o1.getFirst();
                if(a.isNaN()) {
                    a = Double.POSITIVE_INFINITY;
                }
                Double b = o2.getFirst();
                if(b.isNaN()) {
                    b = Double.POSITIVE_INFINITY;
                }
                return b.compareTo(a);  // reverse the result by swapping the sides
            }
        };

        this.comparator = isBottom ? reverseComparator : baseComparator;
    }

    protected void intTermGroup(final String field, final long term, final int group) {
        termGroup(term, group);
    }

    protected void stringTermGroup(final String field, final String term, final int group) {
        termGroup(term, group);
    }

    private void termGroup(final Object term, final int group) {
        PriorityQueue<Pair<Double, GroupStats>> topTerms = groupToTopK.get(group);
        if (topTerms == null) {
            topTerms = new PriorityQueue<Pair<Double, GroupStats>>(topK, comparator);
            groupToTopK.put(group, topTerms);
        }
        final double count = getStat(countStat);
        if (topTerms.size() < topK) {
            topTerms.add(getStats(count, group, term));

            if(++newGroupCount > EZImhotepSession.GROUP_LIMIT) {
                throw new IllegalArgumentException("Number of groups exceeds the limit " +
                        new DecimalFormat("###,###").format(EZImhotepSession.GROUP_LIMIT) +
                        ". Please simplify the query.");
            }
        } else {
            final Double headCount = topTerms.peek().getFirst();
            if ((!isBottom && count > headCount) ||
                    (isBottom && count < headCount) ||
                    (headCount.isNaN() && !Double.isNaN(count))) {
                topTerms.remove();
                topTerms.add(getStats(count, group, term));
            }
        }
    }

    private Pair<Double, GroupStats> getStats(double count, int group, Object term) {
        final double[] stats = new double[statRefs.size()];
        for (int i = 0; i < statRefs.size(); i++) {
            stats[i] = getStat(statRefs.get(i));
        }
        return Pair.of(count, new GroupStats(groupKeys.get(group).add(term), stats));
    }

    public List<GroupStats> getResults() {
        final List<GroupStats> ret = Lists.newArrayList();
        final ArrayDeque<Pair<Double, GroupStats>> stack = new ArrayDeque<Pair<Double, GroupStats>>();
        for (int group = 1; group <= groupKeys.size(); group++) {
            final PriorityQueue<Pair<Double, GroupStats>> pairs = groupToTopK.get(group);
            if (pairs != null) {
                while (!pairs.isEmpty()) {
                    stack.push(pairs.remove());
                }
                while (!stack.isEmpty()) {
                    ret.add(stack.remove().getSecond());
                }
            } else {    // TODO: do we want these empty rows?
                ret.add(new GroupStats(groupKeys.get(group).add(""), new double[statRefs.size()]));
            }
        }
        return ret;
    }
}
