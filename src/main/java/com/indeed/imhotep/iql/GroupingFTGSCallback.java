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
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.GroupKey;
import com.indeed.imhotep.ez.StatReference;
import gnu.trove.TIntObjectHashMap;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.util.*;

/**
 * @author jplaisance
 */
public final class GroupingFTGSCallback extends EZImhotepSession.FTGSCallback {
    private static final Logger log = Logger.getLogger(GroupingFTGSCallbackNoExplode.class);
    private final List<StatReference> statRefs;
    private final Map<Integer, GroupKey> groupKeys;
    private final List<Object> allTerms = Lists.newArrayList();
    private final TIntObjectHashMap<Map<Object, double[]>> groupToTermsStats = new TIntObjectHashMap<Map<Object, double[]>>();
    private final int termLimit;

    public GroupingFTGSCallback(int numStats, List<StatReference> statRefs, Map<Integer, GroupKey> groupKeys) {
        super(numStats);
        this.statRefs = statRefs;
        this.groupKeys = groupKeys;
        termLimit = EZImhotepSession.GROUP_LIMIT / Math.max(groupKeys.size(), 1);
    }

    protected void intTermGroup(final String field, final long term, final int group) {
        termGroup(term, group);
    }

    protected void stringTermGroup(final String field, final String term, final int group) {
        termGroup(term, group);
    }

    private void termGroup(Object term, int group) {
        final int allTermsCount = allTerms.size();

        if(allTermsCount == 0 || !allTerms.get(allTermsCount-1).equals(term)) {
            allTerms.add(term); // got a new term. relying on terms being passed in sorted order
            if(allTermsCount > termLimit) {
                throw new IllegalArgumentException("Number of groups exceeds the limit " +
                        new DecimalFormat("###,###").format(EZImhotepSession.GROUP_LIMIT) +
                        ". Please simplify the query. " +
                        "Try adding [] suffix to non-first groupings to disable addition of 0 rows. (e.g. 'group by country, lang[]')");
            }
        }
        Map<Object, double[]> groupTerms = groupToTermsStats.get(group);
        if(groupTerms == null) {
            groupTerms = Maps.newHashMap();
            groupToTermsStats.put(group, groupTerms);
        }
        groupTerms.put(term, getStats());
    }

    private double[] getStats() {
        final double[] stats = new double[statRefs.size()];
        for (int i = 0; i < statRefs.size(); i++) {
            stats[i] = getStat(statRefs.get(i));
        }
        return stats;
    }

    public List<GroupStats> getResults() {
        final List<GroupStats> ret = Lists.newArrayList();
        // warning: we are reusing the same array instance for all blank rows to save memory
        final double[] emptyArray = new double[statRefs.size()];

        for (int group = 1; group <= groupKeys.size(); group++) {
            final Map<Object, double[]> termsStats = groupToTermsStats.get(group);

            if(termsStats == null) { // this grouping was skipped by FTGS, so assigning 0 stats to all terms
                for(Object missingTerm : allTerms) {
                    ret.add(new GroupStats(groupKeys.get(group).add(missingTerm), emptyArray));
                }
                continue;
            }

            for(Object term : allTerms) {
                double[] stats = termsStats.get(term);
                if(stats == null) {
                    stats = emptyArray;
                }
                ret.add(new GroupStats(groupKeys.get(group).add(term), stats));
            }
        }
        return ret;
    }
}
