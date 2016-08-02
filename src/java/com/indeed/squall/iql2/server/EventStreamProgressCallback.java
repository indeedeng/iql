package com.indeed.squall.iql2.server;

import com.google.common.base.Optional;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.progress.ProgressCallback;

import java.io.PrintWriter;
import java.util.Map;

public class EventStreamProgressCallback implements ProgressCallback {
    private int completedChunks = 0;
    private final boolean isStream;
    private final PrintWriter outputStream;

    public EventStreamProgressCallback(boolean isStream, PrintWriter outputStream) {
        this.isStream = isStream;
        this.outputStream = outputStream;
    }

    @Override
    public void startSession(Optional<Integer> numCommands) {
        if (isStream) {
            outputStream.println("event: totalsteps");
            outputStream.println("data: " + numCommands.get());
            outputStream.println();
            outputStream.flush();
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
            outputStream.flush();
        }
    }

    @Override
    public void startCommand(Session session, com.indeed.squall.iql2.execution.commands.Command command, boolean streamingToTSV) {
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
    public void endCommand(Session session, com.indeed.squall.iql2.execution.commands.Command command) {
        if (isStream) {
            outputStream.println(": Completed " + command.getClass().getSimpleName());
            outputStream.println();
            outputStream.flush();
        }
        incrementChunksCompleted();
    }
}
