package com.indeed.squall.jql.commands;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.jql.QualifiedPush;
import com.indeed.squall.jql.Session;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IterateHandlers {
    public static <T> List<T> executeMulti(Session session, String field, Collection<IterateHandler<T>> iterateHandlers) throws ImhotepOutOfMemoryException, IOException {
        if (iterateHandlers.isEmpty()) {
            throw new IllegalArgumentException("Must have at least 1 IterateHandler");
        }
        Set<String> scope = null;
        for (final IterateHandler<T> handler : iterateHandlers) {
            if (scope == null) {
                scope = handler.scope();
            } else {
                if (!scope.equals(handler.scope())) {
                    throw new IllegalArgumentException("All scopes must match");
                }
            }
        }
        if (scope == null) {
            throw new IllegalArgumentException("Must be given at least 1 handler!");
        }

        final Map<String, ImhotepSession> sessionsSubset = Maps.newHashMap();
        for (final String s : scope) {
            // session session sessions session
            sessionsSubset.put(s, session.sessions.get(s).session);
        }
        final Set<QualifiedPush> pushes = Sets.newHashSet();
        for (final IterateHandler<T> handler : iterateHandlers) {
            pushes.addAll(handler.requires());
        }
        final Map<QualifiedPush, Integer> metricIndexes = Maps.newHashMap();
        final Map<String, IntList> sessionMetricIndexes = Maps.newHashMap();
        session.pushMetrics(pushes, metricIndexes, sessionMetricIndexes);
        for (final IterateHandler<T> handler : iterateHandlers) {
            handler.register(metricIndexes, session.groupKeys);
        }

        if (session.isIntField(field)) {
            final List<Session.IntIterateCallback> intCallbacks = Lists.newArrayList();
            for (final IterateHandler<T> handler : iterateHandlers) {
                intCallbacks.add(handler.intIterateCallback());
            }
            final Session.IntIterateCallback callback = new MultiIntIterateCallback(intCallbacks);
            Session.iterateMultiInt(sessionsSubset, sessionMetricIndexes, field, callback);
        } else if (session.isStringField(field)) {
            final List<Session.StringIterateCallback> stringCallbacks = Lists.newArrayList();
            for (final IterateHandler<T> handler : iterateHandlers) {
                stringCallbacks.add(handler.stringIterateCallback());
            }
            final Session.StringIterateCallback callback = new MultiStringIterateCallback(stringCallbacks);
            Session.iterateMultiString(sessionsSubset, sessionMetricIndexes, field, callback);
        } else {
            for (final Map.Entry<String, Session.ImhotepSessionInfo> s : session.sessions.entrySet()) {
                final String name = s.getKey();
                final boolean isIntField = s.getValue().intFields.contains(field);
                final boolean isStringField = s.getValue().stringFields.contains(field);
                System.out.println("name = " + name + ", isIntField=" + isIntField + ", isStringField=" + isStringField);
            }
            throw new IllegalStateException("Field is neither all int nor all string field: " + field);
        }

        final List<T> result = Lists.newArrayListWithCapacity(iterateHandlers.size());
        for (final IterateHandler<T> iterateHandler : iterateHandlers) {
            result.add(iterateHandler.finish());
        }

        session.popStats();

        return result;
    }

    public static <T> T executeSingle(Session session, String field, IterateHandler<T> iterateHandler) throws IOException, ImhotepOutOfMemoryException {
        return executeMulti(session, field, Collections.singletonList(iterateHandler)).get(0);
    }
    
    public static class MultiIntIterateCallback implements Session.IntIterateCallback {
        private final Session.IntIterateCallback[] callbacks;

        public MultiIntIterateCallback(Collection<Session.IntIterateCallback> callbacks) {
            this.callbacks = callbacks.toArray(new Session.IntIterateCallback[callbacks.size()]);
        }

        @Override
        public void term(long term, long[] stats, int group) {
            for (final Session.IntIterateCallback callback : callbacks) {
                callback.term(term, stats, group);
            }
        }
    } 
    
    public static class MultiStringIterateCallback implements Session.StringIterateCallback {
        private final Session.StringIterateCallback[] callbacks;

        public MultiStringIterateCallback(Collection<Session.StringIterateCallback> callbacks) {
            this.callbacks = callbacks.toArray(new Session.StringIterateCallback[callbacks.size()]);
        }

        @Override
        public void term(String term, long[] stats, int group) {
            for (final Session.StringIterateCallback callback : callbacks) {
                callback.term(term, stats, group);
            }
        }
    } 
}
