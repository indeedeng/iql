package com.indeed.squall.iql2.execution.commands;

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
    private final RegroupConditionMessage FAKE_CONDITION = RegroupConditionMessage.newBuilder().setField("fakeField").setInequality(false).setIntType(true).setIntTerm(0).build();

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final int numSessions = session.sessions.size();
        // TODO: Parallelize
        int index = 0;
        final List<String> sessionNames = new ArrayList<>();
        // TODO: Add group() stat to Imhotep and regroup on (group() - 1) * N + 1
        final GroupMultiRemapMessage[] rules = new GroupMultiRemapMessage[session.numGroups];
        for (final Map.Entry<String, Session.ImhotepSessionInfo> entry : session.sessions.entrySet()) {
            sessionNames.add(entry.getKey());
            final ImhotepSession s = entry.getValue().session;

            session.timer.push("make dumb rules");
            final GroupMultiRemapMessage.Builder builder = GroupMultiRemapMessage.newBuilder();
            builder.addPositiveGroup(0);
            builder.addCondition(FAKE_CONDITION);
            for (int i = 0; i < rules.length; i++) {
                final int target = i + 1;
                final int newGroup = (target - 1) * numSessions + (index + 1);
                builder.setTargetGroup(target);
                builder.setPositiveGroup(0, newGroup);
                builder.setNegativeGroup(newGroup);
                rules[i] = builder.build();
            }
            session.timer.pop();

            session.timer.push("apply dumb rules");
            s.regroupWithProtos(rules, false);
            session.timer.pop();

            index += 1;
        }
        session.assumeDense(new SessionNameGroupKeySet(session.groupKeySet, sessionNames));
    }
}
