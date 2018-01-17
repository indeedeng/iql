package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.TimeUnit;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.DayOfWeekGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.sets.DayOfWeekGroupKeySet;
import org.joda.time.DateTime;

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
        session.checkGroupLimit(session.numGroups * 7);

        final long start = new DateTime(session.getEarliestStart()).withTimeAtStartOfDay().getMillis();
        final long end = new DateTime(session.getLatestEnd()).plusDays(1).withTimeAtStartOfDay().getMillis();
        session.timer.push("daily regroup");
        final int numGroups = session.performTimeRegroup(start, end, TimeUnit.DAY.millis, Optional.<String>absent(), false);
        session.checkGroupLimit(numGroups);
        session.timer.pop();

        session.timer.push("compute remapping");
        final int numBuckets = (int) ((end - start) / TimeUnit.DAY.millis);
        final GroupMultiRemapMessage[] rules = new GroupMultiRemapMessage[numGroups];
        final RegroupConditionMessage fakeCondition = RegroupConditionMessage.newBuilder()
                .setField("fakeField")
                .setIntType(true)
                .setIntTerm(0)
                .setInequality(false)
                .build();
        for (int group = 1; group <= numGroups; group++) {
            final int oldGroup = 1 + (group - 1) / numBuckets;
            final int dayOffset = (group - 1) % numBuckets;
            final long groupStart = start + dayOffset * TimeUnit.DAY.millis;
            final int newGroup = 1 + ((oldGroup - 1) * DAY_KEYS.length) + new DateTime(groupStart).getDayOfWeek() - 1;
            rules[group - 1] = GroupMultiRemapMessage.newBuilder()
                    .setTargetGroup(group)
                    .setNegativeGroup(newGroup)
                    .addCondition(fakeCondition)
                    .addPositiveGroup(newGroup)
                    .build();
        }
        session.timer.pop();
        session.timer.push("shuffle regroup");
        session.regroupWithProtos(rules, true);
        session.timer.pop();
        session.assumeDense(new DayOfWeekGroupKeySet(session.groupKeySet));

        out.accept("success");
    }
}
