package com.indeed.squall.jql.commands;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Session;

public class ExplodeTimeBuckets {
    private final int numBuckets;
    private final Optional<String> timeField;
    private final Optional<String> timeFormat;

    public ExplodeTimeBuckets(int numBuckets, Optional<String> timeField, Optional<String> timeFormat) {
        this.numBuckets = numBuckets;
        this.timeField = timeField;
        this.timeFormat = timeFormat;
    }

    public void execute(Session session) throws ImhotepOutOfMemoryException {
        final long earliestStart = session.getEarliestStart();
        final long latestEnd = session.getLatestEnd();
        final long bucketSize = (latestEnd - earliestStart) / numBuckets;
        new TimePeriodRegroup(bucketSize, timeField, timeFormat).execute(session);
    }
}
