package com.indeed.squall.iql2.execution.progress;

import com.google.common.base.Optional;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.commands.Command;

import java.util.Map;

public interface ProgressCallback {
    /**
     * Optional.absent() when a Command List is not given.
     */
    void startSession(Optional<Integer> numCommands);

    void sessionsOpened(Map<String, Session.ImhotepSessionInfo> sessions);

    void startCommand(Command command, boolean streamingToTSV);

    void endCommand(Command command);
}
