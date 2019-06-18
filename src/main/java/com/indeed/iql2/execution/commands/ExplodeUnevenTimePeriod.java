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
import com.indeed.iql2.execution.groupkeys.sets.UnevenPeriodGroupKeySet;
import com.indeed.iql2.language.TimeUnit;
import com.indeed.iql2.language.query.UnevenGroupByPeriod;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import org.joda.time.DateTime;

import java.util.Optional;

public class ExplodeUnevenTimePeriod implements Command {
    private final Optional<FieldSet> timeField;
    private final Optional<String> timeFormat;
    private final UnevenGroupByPeriod groupByType;

    public ExplodeUnevenTimePeriod(
            final Optional<FieldSet> timeField,
            final Optional<String> timeFormat,
            final UnevenGroupByPeriod groupByType
    ) {
        this.timeField = timeField;
        this.timeFormat = timeFormat;
        this.groupByType = groupByType;
    }

    @Override
    public void execute(final Session session) throws ImhotepOutOfMemoryException {
        final long earliestStart = session.getEarliestStart();
        final long latestEnd = session.getLatestEnd();

        final long unitSize = TimeUnit.DAY.millis;
        final long difference = latestEnd - earliestStart;
        final long realEnd;
        if (difference % unitSize == 0) {
            realEnd = latestEnd;
        } else {
            realEnd = latestEnd + (unitSize - difference % unitSize);
        }

        final int oldNumGroups = session.getNumGroups();

        final int numBuckets = (int) Math.ceil(((double) realEnd - earliestStart) / unitSize);
        final DateTime start = groupByType.startOfPeriod(new DateTime(earliestStart, session.timeZone));
        // end is exclusive, and we're looking for the exclusive end period,
        // so find end of the period that contains (end - 1 second)
        final DateTime endExclusive = groupByType.endOfPeriod(new DateTime(realEnd, session.timeZone).minusSeconds(1));
        final int numPeriods = groupByType.periodsBetween(start, endExclusive);
        session.checkGroupLimit((long) (numPeriods) * session.getNumGroups());

        final long numGroupsLong = session.performTimeRegroup(earliestStart, realEnd, unitSize, timeField, false, false);
        session.checkGroupLimit(numGroupsLong);

        session.timer.push("compute month remapping");
        final int[] fromGroups = new int[oldNumGroups * numBuckets];
        final int[] toGroups = new int[oldNumGroups * numBuckets];
        int index = 0;
        for (int outerGroup = 1; outerGroup <= oldNumGroups; outerGroup++) {
            for (int innerGroup = 0; innerGroup < numBuckets; innerGroup++) {
                final long startTimestamp = earliestStart + innerGroup * unitSize;
                final int base = 1 + (outerGroup - 1) * numBuckets + innerGroup;
                final int newBase = 1 + (outerGroup - 1) * numPeriods;

                final DateTime date = groupByType.startOfPeriod(new DateTime(startTimestamp, session.timeZone));
                final int newGroup = newBase + groupByType.periodsBetween(start, date);
                fromGroups[index] = base;
                toGroups[index] = newGroup;
                index++;
            }
        }
        session.timer.pop();

        session.remapGroups(fromGroups, toGroups);

        final UnevenPeriodGroupKeySet groupKeySet = new UnevenPeriodGroupKeySet(
                session.groupKeySet,
                numPeriods,
                start,
                groupByType,
                timeFormat.orElse(TimeUnit.SECOND.formatString),
                session.formatter,
                session.timeZone
        );
        session.assumeDense(groupKeySet);
    }
}
