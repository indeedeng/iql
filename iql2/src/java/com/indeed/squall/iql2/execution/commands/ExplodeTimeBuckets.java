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
