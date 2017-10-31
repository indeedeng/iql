package com.indeed.squall.iql2.execution.commands;

import com.google.common.primitives.Ints;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.sets.SessionNameGroupKeySet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExplodeSessionNames implements Command {
    private final RegroupConditionMessage FAKE_CONDITION = RegroupConditionMessage.newBuilder()
            .setField("fakeField")
            .setIntType(true)
            .setIntTerm(0L)
            .setInequality(false)
            .build();

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final int numSessions = session.sessions.size();
        // TODO: Parallelize
        int index = 0;
        final List<String> sessionNames = new ArrayList<>();
        // TODO: Add group() stat to Imhotep and regroup on (group() - 1) * N + 1
        final GroupMultiRemapMessage[] messages = new GroupMultiRemapMessage[session.numGroups];
        for (final Map.Entry<String, Session.ImhotepSessionInfo> entry : session.sessions.entrySet()) {
            sessionNames.add(entry.getValue().displayName);
            final ImhotepSession s = entry.getValue().session;

            session.timer.push("make dumb rules");
            for (int i = 0; i < messages.length; i++) {
                final int target = i + 1;
                final int newGroup = (target - 1) * numSessions + (index + 1);
                messages[i] = GroupMultiRemapMessage.newBuilder()
                        .setTargetGroup(target)
                        .setNegativeGroup(newGroup)
                        .addAllPositiveGroup(Ints.asList(new int[] {newGroup}))
                        .addCondition(FAKE_CONDITION)
                        .build();
            }
            session.timer.pop();

            session.timer.push("apply dumb rules");
            s.regroupWithProtos(messages, false);
            session.timer.pop();

            index += 1;
        }
        session.assumeDense(new SessionNameGroupKeySet(session.groupKeySet, sessionNames));
    }
}
