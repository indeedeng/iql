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
import com.indeed.iql2.execution.groupkeys.DayOfWeekGroupKey;
import com.indeed.iql2.execution.groupkeys.sets.DayOfWeekGroupKeySet;
import org.joda.time.DateTime;

import java.util.function.Consumer;

;

public class ExplodeDayOfWeek implements Command {
    public static final String[] DAY_KEYS = { "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday" };

    public static final DayOfWeekGroupKey[] DAY_GROUP_KEYS = new DayOfWeekGroupKey[DAY_KEYS.length];
    static {
        for (int i = 0; i < DAY_GROUP_KEYS.length; i++) {
            DAY_GROUP_KEYS[i] = new DayOfWeekGroupKey(i);
        }
    }

    @Override
    public void execute(final Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        execute(session);
        out.accept("success");
    }

    public void execute(final Session session) throws ImhotepOutOfMemoryException {
        session.checkGroupLimit(session.numGroups * 7);

        final long start = new DateTime(session.getEarliestStart()).withTimeAtStartOfDay().getMillis();
        final long end = new DateTime(session.getLatestEnd()).plusDays(1).withTimeAtStartOfDay().getMillis();
        session.timer.push("daily regroup");
        final int numGroups = session.performTimeRegroup(start, end, TimeUnit.DAY.millis, Optional.<String>absent(), false);
        session.checkGroupLimit(numGroups);
        session.timer.pop();

        session.timer.push("compute remapping");
        final int numBuckets = (int) ((end - start) / TimeUnit.DAY.millis);
        final int[] fromGroups = new int[numGroups];
        final int[] toGroups = new int[numGroups];
        for (int group = 1; group <= numGroups; group++) {
            final int oldGroup = 1 + (group - 1) / numBuckets;
            final int dayOffset = (group - 1) % numBuckets;
            final long groupStart = start + dayOffset * TimeUnit.DAY.millis;
            final int newGroup = 1 + ((oldGroup - 1) * DAY_KEYS.length) + new DateTime(groupStart).getDayOfWeek() - 1;
            fromGroups[group - 1] = group;
            toGroups[group - 1] = newGroup;
        }
        session.timer.pop();
        session.timer.push("shuffle regroup");
        session.remapGroups(fromGroups, toGroups);
        session.timer.pop();
        session.assumeDense(new DayOfWeekGroupKeySet(session.groupKeySet));
    }
}
