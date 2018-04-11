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

package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;

public class ExplodeTimeBuckets implements Command {
    public final int numBuckets;
    public final Optional<String> timeField;
    public final Optional<String> timeFormat;

    public ExplodeTimeBuckets(int numBuckets, Optional<String> timeField, Optional<String> timeFormat) {
        this.numBuckets = numBuckets;
        this.timeField = timeField;
        this.timeFormat = timeFormat;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        final long earliestStart = session.getEarliestStart();
        final long latestEnd = session.getLatestEnd();
        final long bucketSize = (latestEnd - earliestStart) / numBuckets;
        new TimePeriodRegroup(bucketSize, timeField, timeFormat, false).execute(session, out);
    }
}
