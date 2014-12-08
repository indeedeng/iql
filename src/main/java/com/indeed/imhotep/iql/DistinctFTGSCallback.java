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

import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.GroupKey;
import gnu.trove.TIntIntHashMap;

import java.util.Map;

/**
 * @author vladimir
 */

public class DistinctFTGSCallback extends EZImhotepSession.FTGSCallback {
    private final TIntIntHashMap groupToCounts;

    public DistinctFTGSCallback(int numStats, Map<Integer, GroupKey> groupKeys) {
        super(numStats);

        groupToCounts  = new TIntIntHashMap(groupKeys.size());
    }

    @Override
    protected void intTermGroup(String field, long term, int group) {
        incrementGroupCounts(group);
    }

    private void incrementGroupCounts(int group) {
        groupToCounts.put(group, groupToCounts.get(group) + 1);
    }

    @Override
    protected void stringTermGroup(String field, String term, int group) {
        incrementGroupCounts(group);
    }

    /**
     * Returns map of group numbers to field term counts
     */
    public TIntIntHashMap getResults() {
        return groupToCounts;
    }
}
