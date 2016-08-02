package com.indeed.squall.iql2.execution.progress;

import com.google.common.base.Optional;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.commands.Command;

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
    public void startSession(Optional<Integer> numCommands) {
        for (final ProgressCallback progressCallback : progressCallbacks) {
            progressCallback.startSession(numCommands);
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
