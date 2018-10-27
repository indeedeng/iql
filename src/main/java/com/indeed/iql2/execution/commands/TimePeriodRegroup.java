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

package com.indeed.iql2.execution.commands;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.groupkeys.sets.DateTimeRangeGroupKeySet;

public class TimePeriodRegroup implements Command {
    public final long periodMillis;
    public final Optional<String> timeField;
    public final Optional<String> timeFormat;
    public final boolean isRelative;

    public TimePeriodRegroup(long periodMillis, Optional<String> timeField, Optional<String> timeFormat, boolean isRelative) {
        this.periodMillis = periodMillis;
        this.timeField = timeField;
        this.timeFormat = timeFormat;
        this.isRelative = isRelative;
    }

    @Override
    public void execute(final Session session) throws ImhotepOutOfMemoryException {
        final long shardStart;
        final long shardEnd;
        if (!isRelative) {
            shardStart = session.getEarliestStart();
            final long end = session.getLatestEnd();
            if ((end - shardStart) % periodMillis == 0) {
                shardEnd = end;
            } else {
                shardEnd = end + periodMillis - (end- shardStart) % periodMillis;
            }
        } else {
            shardStart = session.getFirstStartTimeMillis();
            long longestDistance = session.getLongestSessionDistance();
            if (longestDistance % periodMillis != 0) {
                longestDistance = longestDistance + periodMillis - longestDistance % periodMillis;
            }
            shardEnd = shardStart + longestDistance;
        }

        final int numBuckets = (int)((shardEnd - shardStart)/periodMillis);
        session.checkGroupLimit(numBuckets * session.numGroups);
        session.performTimeRegroup(shardStart, shardEnd, periodMillis, timeField, isRelative);
        final String format = timeFormat.or("yyyy-MM-dd HH:mm:ss");
        final DateTimeRangeGroupKeySet groupKeySet = new DateTimeRangeGroupKeySet(session.groupKeySet, shardStart, periodMillis, numBuckets, format);
        session.assumeDense(groupKeySet);
    }

}
