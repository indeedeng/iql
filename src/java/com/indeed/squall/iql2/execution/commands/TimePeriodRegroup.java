package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.common.util.Pair;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import org.joda.time.DateTime;

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
            realEnd = shardEnd + (shardEnd - earliestStart) % periodMillis;
        }
        final int numGroups = session.performTimeRegroup(earliestStart, realEnd, periodMillis, timeField);
        final int numBuckets = (int) ((realEnd - earliestStart) / periodMillis);
        final String format = timeFormat.or("yyyy-MM-dd HH:mm:ss");
        session.assumeDense(new Function<Integer, Pair<String, Session.GroupKey>>() {
            public Pair<String, Session.GroupKey> apply(Integer group) {
                final int oldGroup = 1 + (group - 1) / numBuckets;
                final int groupOffset = group - 1 - ((oldGroup - 1) * numBuckets);
                final long start = earliestStart + groupOffset * periodMillis;
                final long end = earliestStart + (groupOffset + 1) * periodMillis;
                return Pair.of("[" + new DateTime(start).toString(format) + ", " + new DateTime(end).toString(format) + ")", session.groupKeys.get(oldGroup));
            }
        }, numGroups);
        session.currentDepth += 1;

        out.accept("TimePeriodRegrouped");
    }
}
