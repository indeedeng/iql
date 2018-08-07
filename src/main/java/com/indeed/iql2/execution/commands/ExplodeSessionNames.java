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

package com.indeed.iql2.execution.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.iql2.execution.Session;
import java.util.function.Consumer;;
import com.indeed.iql2.execution.groupkeys.sets.SessionNameGroupKeySet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExplodeSessionNames implements Command {
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
                        .build();
            }
            session.timer.pop();

            session.timer.push("regroupWithProtos(" + messages.length + " rules)");
            s.regroupWithProtos(messages, false);
            session.timer.pop();

            index += 1;
        }
        session.assumeDense(new SessionNameGroupKeySet(session.groupKeySet, sessionNames));
    }
}
