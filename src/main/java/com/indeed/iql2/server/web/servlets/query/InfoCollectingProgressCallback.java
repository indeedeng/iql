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

package com.indeed.iql2.server.web.servlets.query;

import com.google.common.collect.ImmutableList;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.Command;
import com.indeed.iql2.execution.progress.ProgressCallback;
import com.indeed.iql2.language.query.Queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InfoCollectingProgressCallback implements ProgressCallback {
    private final List<String> sessionIds = new ArrayList<>();
    private int maxNumGroups = 0;

    public List<String> getSessionIds() {
        return ImmutableList.copyOf(sessionIds);
    }

    @Override
    public void sessionsOpened(Map<String, Session.ImhotepSessionInfo> sessions) {
        for (final Session.ImhotepSessionInfo sessionInfo : sessions.values()) {
            final ImhotepSession session = sessionInfo.session;
            final String sessionId = session.getSessionId();
            if (sessionId != null) {
                sessionIds.add(sessionId);
            }
        }
    }

    @Override
    public void queryIdAssigned(final long queryId) {
    }

    @Override
    public void startSession(Optional<Integer> numCommands) {
    }

    @Override
    public void preSessionOpen(final List<Queries.QueryDataset> datasets) {
        // do nothing
    }

    @Override
    public void sessionOpened(final ImhotepSession session) {
    }

    @Override
    public void startCommand(Session session, Command command, boolean streamingToTSV) {
        if (session != null) {
            maxNumGroups = Math.max(maxNumGroups, session.numGroups);
        }
    }

    @Override
    public void endCommand(Session session, Command command) {
        if (session != null) {
            maxNumGroups = Math.max(maxNumGroups, session.numGroups);
        }
    }

    public int getMaxNumGroups() {
        return maxNumGroups;
    }
}
