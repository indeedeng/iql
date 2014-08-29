package com.indeed.imhotep.iql;

import com.indeed.imhotep.ez.GroupKey;
import org.apache.log4j.Logger;

/**
* @author jplaisance
*/
public final class GroupStats {
    final GroupKey groupKey;
    final double[] stats;

    public GroupStats(final GroupKey groupKey, final double[] stats) {
        this.groupKey = groupKey;
        this.stats = stats;
    }

    public GroupKey getGroupKey() {
        return groupKey;
    }

    public double[] getStats() {
        return stats;
    }
}
