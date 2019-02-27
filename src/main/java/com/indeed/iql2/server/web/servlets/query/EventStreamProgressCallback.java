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

import com.indeed.iql.exceptions.OutputStreamFlushException;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.Command;
import com.indeed.iql2.execution.progress.ProgressCallback;
import com.indeed.iql2.language.query.Queries;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class EventStreamProgressCallback implements ProgressCallback {
    private int completedChunks = 0;
    private final boolean isStream;
    private final PrintWriter outputStream;

    public EventStreamProgressCallback(boolean isStream, PrintWriter outputStream) {
        this.isStream = isStream;
        this.outputStream = outputStream;
    }

    private void doFlush() {
        final boolean error = outputStream.checkError();
        if (error) {
            throw new OutputStreamFlushException("Error encountered writing to text/event-stream output");
        }
    }

    @Override
    public void queryIdAssigned(final long queryId) {
        if (!isStream) {
            return;
        }
        outputStream.println("event: queryid");
        outputStream.println("data: " + queryId);
        outputStream.println();
        doFlush();
    }

    @Override
    public void startSession(Optional<Integer> numCommands) {
        if (isStream) {
            outputStream.println("event: totalsteps");
            outputStream.println("data: " + numCommands.get());
            outputStream.println();
            doFlush();
        }
    }

    @Override
    public void preSessionOpen(final List<Queries.QueryDataset> datasets) {
        // do nothing
    }

    @Override
    public void sessionOpened(final ImhotepSessionHolder session) {
        if (!isStream) {
            return;
        }
        final String sessionId = session.getSessionId();
        if (sessionId != null) {
            outputStream.println("event: sessionid");
            outputStream.println("data: " + sessionId);
            outputStream.println();
            doFlush();
        }
    }

    @Override
    public void sessionsOpened(Map<String, Session.ImhotepSessionInfo> sessions) {
        // do nothing
    }

    private void incrementChunksCompleted() {
        completedChunks += 1;
        if (isStream) {
            outputStream.println("event: chunkcomplete");
            outputStream.println("data: " + completedChunks);
            outputStream.println();
            doFlush();
        }
    }

    @Override
    public void startCommand(Session session, Command command, boolean streamingToTSV) {
        if (command != null && isStream) {
            outputStream.println(": Starting " + command.getClass().getSimpleName());
            outputStream.println();
        }

        if (streamingToTSV) {
            incrementChunksCompleted();

            if (isStream) {
                outputStream.println("event: resultstream");
            }
        }
    }

    @Override
    public void endCommand(Session session, Command command) {
        if (isStream) {
            outputStream.println(": Completed " + command.getClass().getSimpleName());
            outputStream.println();
            doFlush();
        }
        incrementChunksCompleted();
    }
}
