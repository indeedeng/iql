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

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.groupkeys.sets.DateTimeRangeGroupKeySet;
import com.indeed.iql2.language.TimeUnit;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;

import java.util.Optional;

public class TimePeriodRegroup implements Command {
    public final long periodMillis;
    public final Optional<FieldSet> timeField;
    public final Optional<String> timeFormat;
    public final boolean isRelative;

    public TimePeriodRegroup(long periodMillis, Optional<FieldSet> timeField, Optional<String> timeFormat, boolean isRelative) {
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
                shardEnd = end + periodMillis - (end - shardStart) % periodMillis;
            }
        } else {
            shardStart = session.getFirstStartTimeMillis();
            long longestDistance = session.getLongestSessionDistance();
            if (longestDistance % periodMillis != 0) {
                longestDistance = longestDistance + periodMillis - longestDistance % periodMillis;
            }
            shardEnd = shardStart + longestDistance;
        }

        final long numBucketsLong = (shardEnd - shardStart) / periodMillis;
        session.checkGroupLimit(numBucketsLong * session.getNumGroups());
        final int numBuckets = (int) numBucketsLong;
        final boolean deleteEmptyGroups = (session.iqlVersion == 1) && !isRelative;
        final long groupCountLong = session.performTimeRegroup(shardStart, shardEnd, periodMillis, timeField, isRelative, deleteEmptyGroups);
        final int groupCount = session.checkGroupLimit(groupCountLong);
        final String format = timeFormat.orElse(TimeUnit.SECOND.formatString);
        final DateTimeRangeGroupKeySet groupKeySet = new DateTimeRangeGroupKeySet(
                session.groupKeySet,
                shardStart,
                periodMillis,
                numBuckets,
                groupCount,
                format,
                session.formatter,
                session.timeZone
        );
        session.assumeDense(groupKeySet);
    }

}
