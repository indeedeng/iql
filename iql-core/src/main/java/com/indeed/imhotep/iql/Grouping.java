package com.indeed.imhotep.iql;

import com.google.common.collect.Lists;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.GroupKey;
import com.indeed.imhotep.ez.StatReference;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author jplaisance
 */
public abstract class Grouping {
    private static final Logger log = Logger.getLogger(Grouping.class);

    public abstract Map<Integer, GroupKey> regroup(EZImhotepSession session, Map<Integer, GroupKey> groupKeys) throws ImhotepOutOfMemoryException;

    public Iterator<GroupStats> getGroupStats(EZImhotepSession session, Map<Integer, GroupKey> groupKeys, List<StatReference> statRefs, long timeoutTS) throws ImhotepOutOfMemoryException {
        groupKeys = regroup(session, groupKeys);
        final int statCount = statRefs.size();
        final double[][] statGroupValues = new double[statCount][];
        for (int i = 0; i < statCount; i++) {
            statGroupValues[i] = statRefs.get(i).getGroupStats();
        }
        final int groupCount = statGroupValues[0].length;
        final List<GroupStats> ret = Lists.newArrayListWithCapacity(groupCount);
        for (int group = 1; group < groupCount; group++) {
            final double[] groupStats = new double[statCount];
            for (int statNum = 0; statNum < groupStats.length; statNum++) {
                groupStats[statNum] = statGroupValues[statNum][group];
            }
            ret.add(new GroupStats(groupKeys.get(group), groupStats));
        }
        final double[] emptyGroupStats = new double[statCount];
        for(int group = groupCount; group < groupKeys.size()+1; group++) {
            ret.add(new GroupStats(groupKeys.get(group), emptyGroupStats));
        }
        return ret.iterator();
    }
}
