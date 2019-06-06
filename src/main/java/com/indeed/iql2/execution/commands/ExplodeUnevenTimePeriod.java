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
import com.indeed.iql2.execution.TimeUnit;
import com.indeed.iql2.execution.groupkeys.sets.YearMonthGroupKeySet;
import com.indeed.iql2.language.query.UnevenGroupByPeriod;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Optional;

public class ExplodeUnevenTimePeriod implements Command {
    private static final DateTimeZone IMHOTEP_TIME = DateTimeZone.forOffsetHours(-6);

    private final Optional<FieldSet> timeField;
    private final Optional<String> timeFormat;
    private final UnevenGroupByPeriod groupByType;

    public ExplodeUnevenTimePeriod(final Optional<FieldSet> timeField, final Optional<String> timeFormat, final UnevenGroupByPeriod groupByType) {
        this.timeField = timeField;
        this.timeFormat = timeFormat;
        this.groupByType = groupByType;
    }

    @Override
    public void execute(final Session session) throws ImhotepOutOfMemoryException {
        final long earliestStart = session.getEarliestStart();
        final long latestEnd = session.getLatestEnd();

        final long unitSize = TimeUnit.DAY.millis;
        final long timeOffsetMinutes = 0L;
        final DateTimeZone zone = DateTimeZone.forOffsetHoursMinutes((int) timeOffsetMinutes / 60, (int) timeOffsetMinutes % 60);
        final long realStart = new DateTime(earliestStart, zone).getMillis();
        final long shardsEnd = new DateTime(latestEnd, zone).getMillis();
        final long difference = shardsEnd - realStart;
        final long realEnd;
        if (difference % unitSize == 0) {
            realEnd = shardsEnd;
        } else {
            realEnd = shardsEnd + (unitSize - difference % unitSize);
        }

        final int oldNumGroups = session.numGroups;

        final int numBuckets = (int) Math.ceil(((double) realEnd - realStart) / unitSize);
        final DateTime start = groupByType.startOfPeriod(new DateTime(realStart, IMHOTEP_TIME));
        // end is exclusive, and we're looking for the exclusive end period,
        // so find end of the period that contains (end - 1 second)
        final DateTime endExclusive = groupByType.endOfPeriod(new DateTime(realEnd, IMHOTEP_TIME).minusSeconds(1));
        final int numPeriods = groupByType.periodsBetween(start, endExclusive);
        session.checkGroupLimit((long) (numPeriods) * session.numGroups);

        final long numGroupsLong = session.performTimeRegroup(realStart, realEnd, unitSize, timeField, false, false);
        session.checkGroupLimit(numGroupsLong);

        session.timer.push("compute month remapping");
        final int[] fromGroups = new int[oldNumGroups * numBuckets];
        final int[] toGroups = new int[oldNumGroups * numBuckets];
        int index = 0;
        for (int outerGroup = 1; outerGroup <= oldNumGroups; outerGroup++) {
            for (int innerGroup = 0; innerGroup < numBuckets; innerGroup++) {
                final long startTimestamp = realStart + innerGroup * unitSize;
                final int base = 1 + (outerGroup - 1) * numBuckets + innerGroup;
                final int newBase = 1 + (outerGroup - 1) * numPeriods;

                final DateTime date = groupByType.startOfPeriod(new DateTime(startTimestamp, IMHOTEP_TIME));
                final int newGroup = newBase + groupByType.periodsBetween(start, date);
                fromGroups[index] = base;
                toGroups[index] = newGroup;
                index++;
            }
        }
        session.timer.pop();

        session.remapGroups(fromGroups, toGroups);

        final YearMonthGroupKeySet groupKeySet = new YearMonthGroupKeySet(
                session.groupKeySet,
                numPeriods,
                start,
                groupByType,
                timeFormat.orElse(TimeUnit.SECOND.formatString),
                session.formatter
        );
        session.assumeDense(groupKeySet);
    }
}
