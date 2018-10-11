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

package com.indeed.iql2.execution.progress;

import com.google.common.base.Optional;
import com.indeed.imhotep.Shard;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.Command;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class CompositeProgressCallback implements ProgressCallback {
    private final List<ProgressCallback> progressCallbacks;

    private CompositeProgressCallback(List<ProgressCallback> progressCallbacks) {
        this.progressCallbacks = progressCallbacks;
    }

    public static CompositeProgressCallback create(ProgressCallback... progressCallbacks) {
        return new CompositeProgressCallback(Arrays.asList(progressCallbacks));
    }

    @Override
    public void queryIdAssigned(final long queryId) {
        for (final ProgressCallback progressCallback : progressCallbacks) {
            progressCallback.queryIdAssigned(queryId);
        }
    }

    @Override
    public void startSession(Optional<Integer> numCommands) {
        for (final ProgressCallback progressCallback : progressCallbacks) {
            progressCallback.startSession(numCommands);
        }
    }

    @Override
    public void preSessionOpen(final Map<String, List<Shard>> datasetToChosenShards) {
        for (final ProgressCallback progressCallback : progressCallbacks) {
            progressCallback.preSessionOpen(datasetToChosenShards);
        }
    }

    @Override
    public void sessionOpened(final ImhotepSessionHolder session) {
        for (final ProgressCallback progressCallback : progressCallbacks) {
            progressCallback.sessionOpened(session);
        }
    }

    @Override
    public void sessionsOpened(Map<String, Session.ImhotepSessionInfo> sessions) {
        for (final ProgressCallback progressCallback : progressCallbacks) {
            progressCallback.sessionsOpened(sessions);
        }
    }

    @Override
    public void startCommand(Session session, Command command, boolean streamingToTSV) {
        for (final ProgressCallback progressCallback : progressCallbacks) {
            progressCallback.startCommand(session, command, streamingToTSV);
        }
    }

    @Override
    public void endCommand(Session session, Command command) {
        for (final ProgressCallback progressCallback : progressCallbacks) {
            progressCallback.endCommand(session, command);
        }
    }
}
