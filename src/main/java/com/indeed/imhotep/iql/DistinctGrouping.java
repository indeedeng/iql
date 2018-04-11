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
 package com.indeed.imhotep.iql;

import com.google.common.collect.Lists;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;
import com.indeed.imhotep.ez.GroupKey;
import com.indeed.imhotep.ez.StatReference;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author vladimir
 */

public class DistinctGrouping extends Grouping {
    // Fields to get distinct term counts for and their positions in the stats list
    private final List<Field> fields = Lists.newArrayList();
    private final IntList distinctProjectionPositions = new IntArrayList();

    public void addField(Field field, int projectionPosition) {
        fields.add(field);
        distinctProjectionPositions.add(projectionPosition);
    }

    public List<Field> getFields() {
        return Lists.newArrayList(fields);
    }

    @Override
    public Int2ObjectMap<GroupKey> regroup(EZImhotepSession session, Int2ObjectMap<GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        throw new UnsupportedOperationException("DistinctGrouping requires FTGS so always should go last in the list of groupings");
    }

    public Iterator<GroupStats> getGroupStats(final EZImhotepSession session, final Int2ObjectMap<GroupKey> groupKeys, final List<StatReference> statRefs, long timeoutTS) throws ImhotepOutOfMemoryException {
        if(groupKeys.isEmpty()) {   // we don't have any parent groups probably because all docs were filtered out
            return Collections.<GroupStats>emptyList().iterator();
        }
        final int statCount = statRefs.size();
        final int groupCount = groupKeys.size();
        final List<GroupStats> result = Lists.newArrayList();

        // list to set for use in lookups
        final IntSet distinctProjectionPositionsSet = new IntOpenHashSet(distinctProjectionPositions.size());
        for(int pos : distinctProjectionPositions) {
            distinctProjectionPositionsSet.add(pos);
        }

        // TODO: don't auto-get group stats on each FTGS iteration

        // map of groups -> projection positions -> values
        Int2ObjectMap<Int2IntMap> distinctData = getDistinctData(session, groupKeys);


        // get values for the normal stats
        final Int2ObjectMap<double[]> statsResults = (statCount > 0) ? getGroupStatsValues(session, statRefs, groupCount) : null;

        // combine normal stats with distinct counts
        for (int groupNum = 1; groupNum <= groupCount; groupNum++) {
            Int2IntMap groupDistinctData = distinctData.get(groupNum);
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

    private Int2ObjectMap<Int2IntMap> getDistinctData(EZImhotepSession session, Int2ObjectMap<GroupKey> groupKeys) {
        Int2ObjectMap<Int2IntMap> distinctData = new Int2ObjectOpenHashMap<Int2IntMap>();

        // get distinct data
        for(int i = 0; i < fields.size(); i++) {
            final Field field = fields.get(i);
            final int projectionPosition = distinctProjectionPositions.get(i);

            final DistinctFTGSCallback callback = new DistinctFTGSCallback(session.getStackDepth(), groupKeys);
            session.ftgsIterate(Lists.newArrayList(field), callback);
            final Int2IntMap distinctResults = callback.getResults();
            for(int groupNum : groupKeys.keySet()) {
                final int distinctResult = distinctResults.get(groupNum);

                Int2IntMap groupDistinctData = distinctData.get(groupNum);
                if(groupDistinctData == null) {
                    groupDistinctData = new Int2IntOpenHashMap(distinctProjectionPositions.size());
                    distinctData.put(groupNum, groupDistinctData);
                }
                groupDistinctData.put(projectionPosition, distinctResult);
            }
        }
        return distinctData;
    }

    private Int2ObjectMap<double[]> getGroupStatsValues(EZImhotepSession session, List<StatReference> statRefs, int groupCount) {
        final int statCount = statRefs.size();
        final double[][] statGroupValues = new double[statCount][];
        for (int i = 0; i < statCount; i++) {
            statGroupValues[i] = session.getGroupStats(statRefs.get(i));
        }
        final Int2ObjectMap<double[]> ret = new Int2ObjectOpenHashMap<double[]>(groupCount);
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
