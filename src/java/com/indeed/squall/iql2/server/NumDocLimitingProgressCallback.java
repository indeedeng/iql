package com.indeed.squall.iql2.server;

import com.google.common.base.Optional;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.commands.Command;
import com.indeed.squall.iql2.execution.progress.ProgressCallback;
import com.indeed.squall.iql2.server.SessionCollectingProgressCallback;

import java.util.Map;

public class NumDocLimitingProgressCallback implements ProgressCallback {
    private final long docLimit;
    private final ProgressCallback wrapped;

    public NumDocLimitingProgressCallback(long docLimit, ProgressCallback wrapped) {
        this.docLimit = docLimit;
        this.wrapped = wrapped;
    }

    @Override
    public void startSession(Optional<Integer> numCommands) {
        wrapped.startSession(numCommands);
    }

    @Override
    public void sessionsOpened(Map<String, Session.ImhotepSessionInfo> sessions) {
        long docCount = 0L;
        for (final Session.ImhotepSessionInfo sessionInfo : sessions.values()) {
            docCount += sessionInfo.session.getNumDocs();
        }
        if (docCount > docLimit) {
            throw new RuntimeException(String.format(
                    "Opening sessions with a total of %s documents exceeds limit of %s.",
                    docCount,
                    docLimit
            ));
        }
        wrapped.sessionsOpened(sessions);
    }

    @Override
    public void startCommand(Command command, boolean streamingToTSV) {
        wrapped.startCommand(command, streamingToTSV);
    }

    @Override
    public void endCommand(Command command) {
        wrapped.endCommand(command);
    }
}
