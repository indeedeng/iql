package com.indeed.squall.iql2.execution.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandler;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandlerable;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandlers;
import com.indeed.squall.iql2.execution.compat.Consumer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GetFieldMin implements IterateHandlerable<long[]>, Command {
    private static final Logger log = Logger.getLogger(GetFieldMin.class);

    public final Set<String> scope;
    public final String field;

    public GetFieldMin(Set<String> scope, String field) {
        this.scope = scope;
        this.field = field;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final long[] result = IterateHandlers.executeSingle(session, field, iterateHandler(session));
        out.accept(Session.MAPPER.writeValueAsString(result));
    }

    @Override
    public IterateHandler<long[]> iterateHandler(Session session) throws ImhotepOutOfMemoryException, IOException {
        return new IterateHandlerImpl(session.numGroups);
    }

    private class IterateHandlerImpl implements IterateHandler<long[]> {
        private long[] min;

        public IterateHandlerImpl(int numGroups) {
            min = new long[numGroups];
            Arrays.fill(min, Long.MAX_VALUE);
        }

        @Override
        public Set<String> scope() {
            return scope;
        }

        @Override
        public Session.IntIterateCallback intIterateCallback() {
            return new Session.IntIterateCallback() {
                @Override
                public void term(long term, long[] stats, int group) {
                    min[group - 1] = Math.min(min[group - 1], term);
                }
            };
        }

        @Override
        public Session.StringIterateCallback stringIterateCallback() {
            throw new UnsupportedOperationException("Expected int field for GetFieldMin!");
        }

        @Override
        public long[] finish() throws ImhotepOutOfMemoryException, IOException {
            return min;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Collections.emptySet();
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
        }
    }
}
