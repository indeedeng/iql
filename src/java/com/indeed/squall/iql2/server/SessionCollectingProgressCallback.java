package com.indeed.squall.iql2.server;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.api.HasSessionId;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.WrappingImhotepSession;
import com.indeed.squall.iql2.execution.commands.Command;
import com.indeed.squall.iql2.execution.progress.NoOpProgressCallback;
import com.indeed.squall.iql2.execution.progress.ProgressCallback;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SessionCollectingProgressCallback implements ProgressCallback {
    private final ProgressCallback wrapped;
    private final List<String> sessionIds = new ArrayList<>();

    public SessionCollectingProgressCallback(ProgressCallback wrapped) {
        this.wrapped = wrapped;
    }

    public List<String> getSessionIds() {
        return ImmutableList.copyOf(sessionIds);
    }

    @Override
    public void sessionsOpened(Map<String, Session.ImhotepSessionInfo> sessions) {
        for (final Session.ImhotepSessionInfo sessionInfo : sessions.values()) {
            ImhotepSession session = sessionInfo.session;
            while (session instanceof WrappingImhotepSession) {
                session = ((WrappingImhotepSession) session).wrapped();
            }
            if (session instanceof HasSessionId) {
                sessionIds.add(((HasSessionId) session).getSessionId());
            }
        }
        wrapped.sessionsOpened(sessions);
    }

    // Auto-generated delegation:

    @Override
    public void startSession(Optional<Integer> numCommands) {
        wrapped.startSession(numCommands);
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
