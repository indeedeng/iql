package com.indeed.imhotep.iql;

import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.GroupKey;
import com.indeed.imhotep.ez.StatReference;

import java.util.List;
import java.util.Map;

/**
 * @author jplaisance
 */
public final class GroupingFTGSCallbackNoExplode extends EZImhotepSession.FTGSIteratingCallback<GroupStats> {
    private final List<StatReference> statRefs;
    private final Map<Integer, GroupKey> groupKeys;

    public GroupingFTGSCallbackNoExplode(int numStats, List<StatReference> statRefs, Map<Integer, GroupKey> groupKeys) {
        super(numStats);
        this.statRefs = statRefs;
        this.groupKeys = groupKeys;
    }

    public GroupStats intTermGroup(final String field, final long term, final int group) {
        return getStats(group, term);
    }

    public GroupStats stringTermGroup(final String field, final String term, final int group) {
        return getStats(group, term);
    }

    private GroupStats getStats(int group, Object term) {
        final double[] stats = new double[statRefs.size()];
        for (int i = 0; i < statRefs.size(); i++) {
            stats[i] = getStat(statRefs.get(i));
        }
        return new GroupStats(groupKeys.get(group).add(term), stats);
    }
}