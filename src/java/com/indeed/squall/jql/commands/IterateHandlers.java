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
import java.util.stream.Collectors;

public class IterateHandlers {
    public static <T> List<T> executeMulti(Session session, String field, Collection<IterateHandler<T>> iterateHandlers) throws ImhotepOutOfMemoryException, IOException {
        if (iterateHandlers.isEmpty()) {
            throw new IllegalArgumentException("Must have at least 1 IterateHandler");
        }
        final Set<String> scope = iterateHandlers.stream().map(IterateHandler::scope).findFirst().get();
        if (!iterateHandlers.stream().allMatch(x -> x.scope().equals(scope))) {
            throw new IllegalArgumentException("All scopes must match");
        }
        
        final Map<String, ImhotepSession> sessionsSubset = Maps.newHashMap();
        scope.forEach(s -> sessionsSubset.put(s, session.sessions.get(s).session));
        final Set<QualifiedPush> pushes = Sets.newHashSet();
        iterateHandlers.forEach(h -> pushes.addAll(h.requires()));
        final Map<QualifiedPush, Integer> metricIndexes = Maps.newHashMap();
        final Map<String, IntList> sessionMetricIndexes = Maps.newHashMap();
        session.pushMetrics(pushes, metricIndexes, sessionMetricIndexes);
        iterateHandlers.forEach(h -> h.register(metricIndexes, session.groupKeys));

        if (session.isIntField(field)) {
            final List<Session.IntIterateCallback> intCallbacks = iterateHandlers.stream().map(IterateHandler::intIterateCallback).collect(Collectors.toList());
            final Session.IntIterateCallback callback = new MultiIntIterateCallback(intCallbacks);
            Session.iterateMultiInt(sessionsSubset, sessionMetricIndexes, field, callback);
        } else if (session.isStringField(field)) {
            final List<Session.StringIterateCallback> stringCallbacks = iterateHandlers.stream().map(IterateHandler::stringIterateCallback).collect(Collectors.toList());
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

        session.sessions.values().forEach(s -> {
            while (s.session.getNumStats() > 0) {
                s.session.popStat();
            }
        });

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
