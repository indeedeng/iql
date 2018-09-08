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
import com.indeed.iql2.execution.TimeUnit;
import com.indeed.iql2.execution.compat.Consumer;
import com.indeed.iql2.execution.groupkeys.sets.YearMonthGroupKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;

public class ExplodeMonthOfYear implements Command {
    @Override
    public void execute(final Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        final long earliestStart = session.getEarliestStart();
        final long latestEnd = session.getLatestEnd();
        final TimeUnit timeUnit = TimeUnit.MONTH;

        final long unitSize = TimeUnit.DAY.millis;
        final long timeOffsetMinutes = 0L;
        final DateTimeZone zone = DateTimeZone.forOffsetHoursMinutes((int) timeOffsetMinutes / 60, (int) timeOffsetMinutes % 60);
        final long realStart = new DateTime(earliestStart, zone).getMillis();
        final long shardsEnd = new DateTime(latestEnd, zone).getMillis();
        final long difference = shardsEnd - realStart;
        final long realEnd;
        if (difference % timeUnit.millis == 0) {
            realEnd = shardsEnd;
        } else {
            realEnd = shardsEnd + (timeUnit.millis - difference % timeUnit.millis);
        }

        final int oldNumGroups = session.numGroups;

        final int numBuckets = (int)Math.ceil(((double)realEnd - realStart) / unitSize);
        final DateTime startMonth = new DateTime(earliestStart, zone).withDayOfMonth(1).withTimeAtStartOfDay();
        final DateTime endMonthExclusive = new DateTime(latestEnd, zone).minusDays(1).withDayOfMonth(1).withTimeAtStartOfDay().plusMonths(1);
        final int numMonths = Months.monthsBetween(
                startMonth,
                endMonthExclusive
        ).getMonths();
        session.checkGroupLimit(numMonths * session.numGroups);

        final int numGroups = session.performTimeRegroup(realStart, realEnd, unitSize, Optional.<String>absent(), false);
        session.checkGroupLimit(numGroups);

        session.timer.push("compute month remapping");
        final int[] fromGroups = new int[oldNumGroups * numBuckets];
        final int[] toGroups = new int[oldNumGroups * numBuckets];
        int index = 0;
        for (int outerGroup = 1; outerGroup <= oldNumGroups; outerGroup++) {
            for (int innerGroup = 0; innerGroup < numBuckets; innerGroup++) {
                final long start = realStart + innerGroup * unitSize;
                final int base = 1 + (outerGroup - 1) * numBuckets + innerGroup;
                final int newBase = 1 + (outerGroup - 1) * numMonths;

                final DateTime date = new DateTime(start, zone).withDayOfMonth(1).withTimeAtStartOfDay();
                final int newGroup = newBase + Months.monthsBetween(startMonth, date).getMonths();
                fromGroups[index] = base;
                toGroups[index] = newGroup;
                index++;
            }
        }
        session.timer.pop();

        session.remapGroups(fromGroups, toGroups);

        session.assumeDense(new YearMonthGroupKey(session.groupKeySet, numMonths, startMonth, TimeUnit.MONTH.formatString));
    }
}
