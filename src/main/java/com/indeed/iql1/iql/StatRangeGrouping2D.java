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

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql1.ez.EZImhotepSession;
import com.indeed.iql1.ez.GroupKey;
import com.indeed.iql1.ez.SingleStatReference;
import com.indeed.iql.web.Limits;
import com.indeed.iql.exceptions.IqlKnownException;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.log4j.Logger;

import static com.indeed.iql1.ez.Stats.Stat;

/**
 * @author jplaisance
 */
public final class StatRangeGrouping2D extends Grouping {
    private static final Logger log = Logger.getLogger(StatRangeGrouping2D.class);

    private final Stat xStat;
    private final long xMin;
    private final long xMax;
    private final long xIntervalSize;
    private final Stat yStat;
    private final long yMin;
    private final long yMax;
    private final long yIntervalSize;

    public StatRangeGrouping2D(final Stat xStat, final long xMin, final long xMax, final long xIntervalSize,
                               final Stat yStat, final long yMin, final long yMax, final long yIntervalSize,
                               final Limits limits) {
        if(xIntervalSize <= 0) {
            throw new IqlKnownException.ParseErrorException("Bucket size has to be positive for stat: " + xStat.toString());
        }
        if(yIntervalSize <= 0) {
            throw new IqlKnownException.ParseErrorException("Bucket size has to be positive for stat: " + yStat.toString());
        }
        this.xStat = xStat;
        this.xMin = xMin;
        this.xMax = xMax;
        this.xIntervalSize = xIntervalSize;
        this.yStat = yStat;
        this.yMin = yMin;
        this.yMax = yMax;
        this.yIntervalSize = yIntervalSize;

        final long expectedBucketCount = ((long)xMax - xMin) / xIntervalSize + ((long)yMax - yMin) / yIntervalSize;
        if(!limits.satisfiesQueryInMemoryRowsLimit(expectedBucketCount) || expectedBucketCount < 0) {
            throw new IqlKnownException.GroupLimitExceededException("Requested bucket count for metrics " + xStat.toString() + " & " + yStat.toString() +
                    " is " + expectedBucketCount + " which is over the limit of " + limits.queryInMemoryRowsLimit);
        }
    }

    public Int2ObjectMap<GroupKey> regroup(final EZImhotepSession session, final Int2ObjectMap<GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        if(groupKeys.isEmpty()) {
            return groupKeys;
        }
        final SingleStatReference xStatRef = session.pushStat(xStat);
        final SingleStatReference yStatRef = session.pushStat(yStat);
        final Int2ObjectMap<GroupKey> ret = session.metricRegroup2D(xStatRef, xMin, xMax, xIntervalSize, yStatRef, yMin, yMax, yIntervalSize);
        session.popStat();
        session.popStat();
        return ret;
    }
}
