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
