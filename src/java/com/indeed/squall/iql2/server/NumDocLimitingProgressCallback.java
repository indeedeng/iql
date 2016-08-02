package com.indeed.squall.iql2.server;

import com.google.common.base.Optional;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.commands.Command;
import com.indeed.squall.iql2.execution.progress.ProgressCallback;

import java.util.Map;

public class NumDocLimitingProgressCallback implements ProgressCallback {
    private final long docLimit;

    public NumDocLimitingProgressCallback(long docLimit) {
        this.docLimit = docLimit;
    }

    @Override
    public void startSession(Optional<Integer> numCommands) {
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
    }

    @Override
    public void startCommand(Session session, Command command, boolean streamingToTSV) {
    }

    @Override
    public void endCommand(Session session, Command command) {
    }
}
