package com.indeed.iql2.execution.progress;

import com.google.common.base.Optional;
import com.indeed.imhotep.Shard;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.Command;
import io.opentracing.ActiveSpan;

import java.util.List;
import java.util.Map;

/**
 * Allows modifying (setting tags etc on) the given {@code activeSpan} as the query progresses
 */
public class SpanUpdaterProgressCallback implements ProgressCallback {
    private final ActiveSpan activeSpan;

    public SpanUpdaterProgressCallback(final ActiveSpan activeSpan) {
        this.activeSpan = activeSpan;
    }

    @Override
    public void queryIdAssigned(final long queryId) {
        activeSpan.setTag("queryid", queryId);
    }

    @Override
    public void startSession(final Optional<Integer> numCommands) {
    }

    @Override
    public void preSessionOpen(final Map<String, List<Shard>> datasetToChosenShards) {
    }

    @Override
    public void sessionOpened(final ImhotepSessionHolder session) {
    }

    @Override
    public void sessionsOpened(final Map<String, Session.ImhotepSessionInfo> sessions) {
    }

    @Override
    public void startCommand(final Session session, final Command command, final boolean streamingToTSV) {
    }

    @Override
    public void endCommand(final Session session, final Command command) {
    }
}
