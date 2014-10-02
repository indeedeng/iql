package com.indeed.imhotep.iql;

import com.google.common.collect.Lists;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;
import com.indeed.imhotep.ez.GroupKey;
import com.indeed.imhotep.ez.StatReference;
import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntObjectHashMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author vladimir
 */

public class DistinctGrouping extends Grouping {
    // Fields to get distinct term counts for and their positions in the stats list
    private final List<Field> fields = Lists.newArrayList();
    private final List<Integer> distinctProjectionPositions = Lists.newArrayList();

    public void addField(Field field, int projectionPosition) {
        fields.add(field);
        distinctProjectionPositions.add(projectionPosition);
    }

    public List<Field> getFields() {
        return Lists.newArrayList(fields);
    }

    @Override
    public Map<Integer, GroupKey> regroup(EZImhotepSession session, Map<Integer, GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        throw new UnsupportedOperationException("DistinctGrouping requires FTGS so always should go last in the list of groupings");
    }

    public Iterator<GroupStats> getGroupStats(final EZImhotepSession session, final Map<Integer, GroupKey> groupKeys, final List<StatReference> statRefs, long timeoutTS) throws ImhotepOutOfMemoryException {
        final int statCount = statRefs.size();
        final int groupCount = groupKeys.size();
        final List<GroupStats> result = Lists.newArrayList();

        // list to set for use in lookups
        final TIntHashSet distinctProjectionPositionsSet = new TIntHashSet(distinctProjectionPositions.size());
        for(int pos : distinctProjectionPositions) {
            distinctProjectionPositionsSet.add(pos);
        }

        // TODO: don't auto-get group stats on each FTGS iteration

        // map of groups -> projection positions -> values
        TIntObjectHashMap<TIntIntHashMap> distinctData = getDistinctData(session, groupKeys);


        // get values for the normal stats
        final TIntObjectHashMap<double[]> statsResults = (statCount > 0) ? getGroupStatsValues(session, statRefs, groupCount) : null;

        // combine normal stats with distinct counts
        for (int groupNum = 1; groupNum <= groupCount; groupNum++) {
            TIntIntHashMap groupDistinctData = distinctData.get(groupNum);
            double[] statsVals = statsResults != null ? statsResults.get(groupNum) : null;

            double[] values = new double[statCount + fields.size()];
            for(int i = 0, statsValsIndex = 0; i < values.length; i++) {
                if(distinctProjectionPositionsSet.contains(i)) {    // distinct value
                    values[i] = groupDistinctData != null ? groupDistinctData.get(i) : 0;
                } else if(statsVals != null && statsValsIndex < statsVals.length) {
                    values[i] = statsVals[statsValsIndex++];    // normal stat value available
                } else {
                    values[i] = 0;  // normal stat not in stats array
                }
            }

            GroupKey groupKey = groupKeys.get(groupNum);
            result.add(new GroupStats(groupKey, values));
        }

        return result.iterator();
    }

    private TIntObjectHashMap<TIntIntHashMap> getDistinctData(EZImhotepSession session, Map<Integer, GroupKey> groupKeys) {
        TIntObjectHashMap<TIntIntHashMap> distinctData = new TIntObjectHashMap<TIntIntHashMap>();

        // get distinct data
        for(int i = 0; i < fields.size(); i++) {
            final Field field = fields.get(i);
            final int projectionPosition = distinctProjectionPositions.get(i);

            final DistinctFTGSCallback callback = new DistinctFTGSCallback(session.getStackDepth(), groupKeys);
            session.ftgsIterate(Lists.newArrayList(field), callback);
            final TIntIntHashMap distinctResults = callback.getResults();
            for(int groupNum : groupKeys.keySet()) {
                final int distinctResult = distinctResults.get(groupNum);

                TIntIntHashMap groupDistinctData = distinctData.get(groupNum);
                if(groupDistinctData == null) {
                    groupDistinctData = new TIntIntHashMap(distinctProjectionPositions.size());
                    distinctData.put(groupNum, groupDistinctData);
                }
                groupDistinctData.put(projectionPosition, distinctResult);
            }
        }
        return distinctData;
    }

    private TIntObjectHashMap<double[]> getGroupStatsValues(EZImhotepSession session, List<StatReference> statRefs, int groupCount) {
        final int statCount = statRefs.size();
        final double[][] statGroupValues = new double[statCount][];
        for (int i = 0; i < statCount; i++) {
            statGroupValues[i] = session.getGroupStats(statRefs.get(i));
        }
        final TIntObjectHashMap<double[]> ret = new TIntObjectHashMap<double[]>(groupCount);
        for (int group = 1; group <= groupCount; group++) {
            final double[] groupStats = new double[statCount];
            for (int statNum = 0; statNum < groupStats.length; statNum++) {
                if(group < statGroupValues[statNum].length) {
                    groupStats[statNum] = statGroupValues[statNum][group];
                }
            }
            ret.put(group, groupStats);
        }
        return ret;
    }
}
