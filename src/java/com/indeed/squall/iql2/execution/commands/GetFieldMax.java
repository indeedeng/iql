package com.indeed.squall.iql2.execution.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GetFieldMax implements IterateHandlerable<long[]>, Command {
    private static final Logger log = Logger.getLogger(GetFieldMax.class);

    private final Set<String> scope;
    public final String field;

    public GetFieldMax(Set<String> scope, String field) {
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
        private long[] max;

        public IterateHandlerImpl(int numGroups) {
            max = new long[numGroups];
            Arrays.fill(max, Long.MIN_VALUE);
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
                    // Relying on the assumption of FTGS sort order -- last term in each group will be the highest.
                    max[group - 1] = term;
                }
            };
        }

        @Override
        public Session.StringIterateCallback stringIterateCallback() {
            throw new UnsupportedOperationException("Expected int field for GetFieldMax!");
        }

        @Override
        public long[] finish() throws ImhotepOutOfMemoryException, IOException {
            return max;
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
