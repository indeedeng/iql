package com.indeed.squall.iql2.execution.progress;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.commands.Command;

import java.util.Map;

public interface ProgressCallback {
    /**
     * Optional.absent() when a Command List is not given.
     */
    void startSession(Optional<Integer> numCommands);

    // Provided separately in order to allow early termination.
    void sessionOpened(ImhotepSession session);

    void sessionsOpened(Map<String, Session.ImhotepSessionInfo> sessions);

    void startCommand(Session session, Command command, boolean streamingToTSV);

    void endCommand(Session session, Command command);
}
