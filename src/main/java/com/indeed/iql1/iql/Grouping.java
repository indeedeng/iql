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
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql1.ez.EZImhotepSession;
import com.indeed.iql1.ez.GroupKey;
import com.indeed.iql1.ez.StatReference;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author jplaisance
 */
public abstract class Grouping {
    private static final Logger log = Logger.getLogger(Grouping.class);

    public abstract Int2ObjectMap<GroupKey> regroup(EZImhotepSession session, Int2ObjectMap<GroupKey> groupKeys) throws ImhotepOutOfMemoryException;

    public Iterator<GroupStats> getGroupStats(EZImhotepSession session, Int2ObjectMap<GroupKey> groupKeys, List<StatReference> statRefs, long timeoutTS) throws ImhotepOutOfMemoryException {
        if(groupKeys.isEmpty()) {   // we don't have any parent groups probably because all docs were filtered out
            return Collections.<GroupStats>emptyList().iterator();
        }
        groupKeys = regroup(session, groupKeys);
        if(groupKeys.isEmpty()) {   // we ended up with no groups so return the empty result
            return Collections.<GroupStats>emptyList().iterator();
        }
        final int statCount = statRefs.size();
        final double[][] statGroupValues = new double[statCount][];
        for (int i = 0; i < statCount; i++) {
            statGroupValues[i] = statRefs.get(i).getGroupStats();
        }
        final int groupCount = getGroupCount(statGroupValues);
        final List<GroupStats> ret = Lists.newArrayListWithCapacity(Math.max(groupCount, groupKeys.size()));
        for (int group = 1; group < groupCount; group++) {
            final double[] groupStats = new double[statCount];
            for (int statNum = 0; statNum < groupStats.length; statNum++) {
                groupStats[statNum] = (statGroupValues[statNum].length > group) ? statGroupValues[statNum][group] : 0;
            }
            ret.add(new GroupStats(groupKeys.get(group), groupStats));
        }
        final double[] emptyGroupStats = new double[statCount];
        for(int group = Math.max(groupCount, 1); group < (groupKeys.size() + 1); group++) {
            ret.add(new GroupStats(groupKeys.get(group), emptyGroupStats));
        }
        return ret.iterator();
    }

    private int getGroupCount(final double[][] statGroupValues) {
        int groupCount = 0;
        for (final double[] groupValue : statGroupValues) {
            groupCount = Math.max(groupCount, groupValue.length);
        }
        return groupCount;
    }
}
