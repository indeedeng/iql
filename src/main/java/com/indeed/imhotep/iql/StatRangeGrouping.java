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

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.GroupKey;
import com.indeed.imhotep.ez.SingleStatReference;
import com.indeed.util.serialization.Stringifier;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.util.Map;

import static com.indeed.imhotep.ez.Stats.Stat;

/**
 * @author jplaisance
 */
public final class StatRangeGrouping extends Grouping {
    private static final Logger log = Logger.getLogger(StatRangeGrouping.class);

    private final Stat stat;
    private final long minValue;
    private final long maxValue;
    private final long intervalSize;
    private final boolean noGutters;
    private final Stringifier<Long> stringFormatter;
    private final boolean isTimeGrouping;
    private final long expectedBucketCount;
    private final DecimalFormat df = new DecimalFormat("###,###");


    public StatRangeGrouping(final Stat stat, final long minValue, final long maxValue, final long intervalSize,
                             final boolean noGutters, Stringifier<Long> stringFormatter, boolean isTimeGrouping) {
        if(intervalSize <= 0) {
            throw new IllegalArgumentException("Bucket size has to be positive for stat: " + stat.toString());
        }
        this.stat = stat;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.intervalSize = intervalSize;
        this.noGutters = noGutters;
        this.stringFormatter = stringFormatter;
        this.isTimeGrouping = isTimeGrouping;

        expectedBucketCount = (maxValue - minValue) / intervalSize;
        if(expectedBucketCount > EZImhotepSession.GROUP_LIMIT || expectedBucketCount < 0) {
            throw new IllegalArgumentException("Requested bucket count for metric " + this.stat.toString() +
                    " is " + df.format(expectedBucketCount) + " which is over the limit of " + df.format(EZImhotepSession.GROUP_LIMIT));
        }
    }

    public Int2ObjectMap<GroupKey> regroup(final EZImhotepSession session, final Int2ObjectMap<GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        if(groupKeys.isEmpty()) {
            return groupKeys;
        }
        final long expectedNumberOfRows = session.getNumGroups() * expectedBucketCount;
        if(expectedNumberOfRows > EZImhotepSession.GROUP_LIMIT || expectedNumberOfRows < 0) {
            throw new IllegalArgumentException("Expected number of rows after bucketing by " + this.stat.toString() +
                    " is " + df.format(expectedNumberOfRows) + " which is over the limit of " + df.format(EZImhotepSession.GROUP_LIMIT) +
                    " rows in memory. Please optimize the query.");
        }

        final SingleStatReference statRef = session.pushStat(stat);
        boolean noGutters = this.noGutters;
        // Special case for Time regroups:
        // We want gutters enabled when it's the first regroup (we have only one group key) since the empty gutters get
        // truncated then but non-empty gutters are kept to indicate a likely problem with the time field in the index.
        // For non first regroup we need to disable gutters to avoid having them with value 0 for every groupKey since
        // in this case the 0s are not trailing and don't get truncated.
        if(isTimeGrouping) {
            noGutters = groupKeys.size() > 1;
        }
        final Int2ObjectMap<GroupKey> ret = session.metricRegroup(statRef, minValue, maxValue, intervalSize, noGutters, stringFormatter, groupKeys);
        session.popStat();
        return ret;
    }
}
