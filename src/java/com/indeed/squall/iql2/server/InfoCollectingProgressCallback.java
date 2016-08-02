package com.indeed.squall.iql2.server;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.indeed.imhotep.api.HasSessionId;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.WrappingImhotepSession;
import com.indeed.squall.iql2.execution.commands.Command;
import com.indeed.squall.iql2.execution.progress.ProgressCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InfoCollectingProgressCallback implements ProgressCallback {
    private final List<String> sessionIds = new ArrayList<>();
    private long totalNumDocs = 0;
    private int maxNumGroups = 0;

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
            totalNumDocs += session.getNumDocs();
        }
    }

    @Override
    public void startSession(Optional<Integer> numCommands) {
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

    public long getTotalNumDocs() {
        return totalNumDocs;
    }
}
