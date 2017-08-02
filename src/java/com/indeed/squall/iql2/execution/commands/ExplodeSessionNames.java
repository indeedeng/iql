package com.indeed.squall.iql2.execution.commands;

import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.sets.SessionNameGroupKeySet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExplodeSessionNames implements Command {
    private final RegroupCondition FAKE_CONDITION = new RegroupCondition("fakeField", true, 0L, null, false);
    private final RegroupCondition[] CONDITIONS_ARRAY = new RegroupCondition[]{FAKE_CONDITION};

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final int numSessions = session.sessions.size();
        // TODO: Parallelize
        int index = 0;
        final List<String> sessionNames = new ArrayList<>();
        // TODO: Add group() stat to Imhotep and regroup on (group() - 1) * N + 1
        final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[session.numGroups];
        for (final Map.Entry<String, Session.ImhotepSessionInfo> entry : session.sessions.entrySet()) {
            sessionNames.add(entry.getValue().displayName);
            final ImhotepSession s = entry.getValue().session;

            session.timer.push("make dumb rules");
            for (int i = 0; i < rules.length; i++) {
                final int target = i + 1;
                final int newGroup = (target - 1) * numSessions + (index + 1);
                rules[i] = new GroupMultiRemapRule(target, newGroup, new int[]{newGroup}, CONDITIONS_ARRAY);
            }
            session.timer.pop();

            session.timer.push("apply dumb rules");
            s.regroup(rules, false);
            session.timer.pop();

            index += 1;
        }
        session.assumeDense(new SessionNameGroupKeySet(session.groupKeySet, sessionNames));
    }
}
