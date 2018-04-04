package com.indeed.squall.iql2.execution.progress;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.commands.Command;

import java.util.Map;

public class SessionOpenedOnlyProgressCallback implements ProgressCallback {
    private final ProgressCallback wrapped;

    public SessionOpenedOnlyProgressCallback(ProgressCallback wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public void startSession(Optional<Integer> numCommands) {
    }

    @Override
    public void sessionOpened(ImhotepSession session) {
        wrapped.sessionOpened(session);
    }

    @Override
    public void sessionsOpened(Map<String, Session.ImhotepSessionInfo> sessions) {
    }

    @Override
    public void startCommand(Session session, Command command, boolean streamingToTSV) {
    }

    @Override
    public void endCommand(Session session, Command command) {
    }
}
