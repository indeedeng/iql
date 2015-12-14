package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.sets.DateTimeRangeGroupKeySet;

public class TimePeriodRegroup implements Command {
    public final long periodMillis;
    public final Optional<String> timeField;
    public final Optional<String> timeFormat;

    public TimePeriodRegroup(long periodMillis, Optional<String> timeField, Optional<String> timeFormat) {
        this.periodMillis = periodMillis;
        this.timeField = timeField;
        this.timeFormat = timeFormat;
    }

    @Override
    public void execute(final Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        final long earliestStart = session.getEarliestStart();
        final long shardEnd = session.getLatestEnd();
        final long realEnd;
        if ((shardEnd - earliestStart) % periodMillis == 0) {
            realEnd = shardEnd;
        } else {
            realEnd = shardEnd + periodMillis - (shardEnd - earliestStart) % periodMillis;
        }
        final int numBuckets = (int) ((realEnd - earliestStart) / periodMillis);

        session.checkGroupLimit(numBuckets * session.numGroups);

        session.performTimeRegroup(earliestStart, realEnd, periodMillis, timeField);
        final String format = timeFormat.or("yyyy-MM-dd HH:mm:ss");

        session.densify(new DateTimeRangeGroupKeySet(session.groupKeySet, earliestStart, periodMillis, numBuckets, format));
        session.currentDepth += 1;

        out.accept("TimePeriodRegrouped");
    }

}
