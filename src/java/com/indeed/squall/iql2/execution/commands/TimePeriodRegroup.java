package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.DayRangeGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.GroupKeyCreator;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.Map;

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
            realEnd = shardEnd + periodMillis + (earliestStart - shardEnd) % periodMillis;
        }
        final int numGroups = session.performTimeRegroup(earliestStart, realEnd, periodMillis, timeField);
        final int numBuckets = (int) ((realEnd - earliestStart) / periodMillis);
        final String format = timeFormat.or("yyyy-MM-dd HH:mm:ss");

        final Map<Integer, GroupKey> groupOffsetToGroupKey = new Int2ObjectOpenHashMap<>();

        session.assumeDense(new GroupKeyCreator() {
            @Override
            public int parent(int group) {
                return 1 + (group - 1) / numBuckets;
            }

            @Override
            public GroupKey forIndex(int group) {
                final int oldGroup = this.parent(group);
                final int groupOffset = group - 1 - ((oldGroup - 1) * numBuckets);
                if (!groupOffsetToGroupKey.containsKey(groupOffset)) {
                    final long start = earliestStart + groupOffset * periodMillis;
                    final long end = earliestStart + (groupOffset + 1) * periodMillis;
                    groupOffsetToGroupKey.put(groupOffset, new DayRangeGroupKey(format, start, end));
                }
                return groupOffsetToGroupKey.get(groupOffset);
            }
        }, numGroups);
        session.currentDepth += 1;

        out.accept("TimePeriodRegrouped");
    }
}
