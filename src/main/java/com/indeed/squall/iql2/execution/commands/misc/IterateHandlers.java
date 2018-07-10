/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.squall.iql2.execution.commands.misc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IterateHandlers {
    private static final Logger log = Logger.getLogger(IterateHandlers.class);

    public static <T> List<T> executeMulti(Session session, String field, Collection<IterateHandler<T>> iterateHandlers) throws ImhotepOutOfMemoryException, IOException {
        session.timer.push("IterateHandlers.executeMulti");

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

        session.timer.push("get subset");
        final Map<String, ImhotepSession> sessionsSubset = Maps.newHashMap();
        for (final String s : scope) {
            // session session sessions session
            sessionsSubset.put(s, session.sessions.get(s).session);
        }
        session.timer.pop();

        session.timer.push("push and register metrics");
        final Set<QualifiedPush> pushes = Sets.newHashSet();
        for (final IterateHandler<T> handler : iterateHandlers) {
            pushes.addAll(handler.requires());
        }
        final Map<QualifiedPush, Integer> metricIndexes = Maps.newHashMap();
        final Map<String, IntList> sessionMetricIndexes = Maps.newHashMap();
        session.pushMetrics(pushes, metricIndexes, sessionMetricIndexes);
        for (final IterateHandler<T> handler : iterateHandlers) {
            handler.register(metricIndexes, session.groupKeySet);
        }
        session.timer.pop();

        if (session.isIntField(field)) {
            final List<Session.IntIterateCallback> intCallbacks = Lists.newArrayList();
            for (final IterateHandler<T> handler : iterateHandlers) {
                intCallbacks.add(handler.intIterateCallback());
            }
            final Session.IntIterateCallback callback = new MultiIntIterateCallback(intCallbacks);
            session.timer.push("iterateMultiInt");
            Session.iterateMultiInt(sessionsSubset, sessionMetricIndexes, Collections.<String, Integer>emptyMap(), field, callback, session.timer, session.options);
            session.timer.pop();
        } else if (session.isStringField(field)) {
            final List<Session.StringIterateCallback> stringCallbacks = Lists.newArrayList();
            for (final IterateHandler<T> handler : iterateHandlers) {
                stringCallbacks.add(handler.stringIterateCallback());
            }
            final Session.StringIterateCallback callback = new MultiStringIterateCallback(stringCallbacks);
            session.timer.push("iterateMultiString");
            Session.iterateMultiString(sessionsSubset, sessionMetricIndexes, Collections.<String, Integer>emptyMap(), field, callback, session.timer, session.options);
            session.timer.pop();
        } else {
            if (log.isDebugEnabled()) {
                for (final Map.Entry<String, Session.ImhotepSessionInfo> s : session.sessions.entrySet()) {
                    final String name = s.getKey();
                    final boolean isIntField = s.getValue().intFields.contains(field);
                    final boolean isStringField = s.getValue().stringFields.contains(field);
                    log.debug("name = " + name + ", isIntField=" + isIntField + ", isStringField=" + isStringField);
                }
            }
            throw new IllegalStateException("Field is neither all int nor all string field: " + field);
        }

        session.timer.push("IterateHandler::finish");
        final List<T> result = Lists.newArrayListWithCapacity(iterateHandlers.size());
        for (final IterateHandler<T> iterateHandler : iterateHandlers) {
            result.add(iterateHandler.finish());
        }
        session.timer.pop();

        session.popStats();

        session.timer.pop();

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
        public void term(final long term, final long[] stats, final int group) {
            for (final Session.IntIterateCallback callback : callbacks) {
                callback.term(term, stats, group);
            }
        }

        @Override
        public boolean needSorted() {
            return Arrays.stream(callbacks).anyMatch(Session.IntIterateCallback::needSorted);
        }

        @Override
        public boolean needGroup() {
            return Arrays.stream(callbacks).anyMatch(Session.IntIterateCallback::needGroup);
        }

        @Override
        public boolean needStats() {
            return Arrays.stream(callbacks).anyMatch(Session.IntIterateCallback::needStats);
        }
    } 
    
    public static class MultiStringIterateCallback implements Session.StringIterateCallback {
        private final Session.StringIterateCallback[] callbacks;

        public MultiStringIterateCallback(Collection<Session.StringIterateCallback> callbacks) {
            this.callbacks = callbacks.toArray(new Session.StringIterateCallback[callbacks.size()]);
        }

        @Override
        public void term(final String term, final long[] stats, final int group) {
            for (final Session.StringIterateCallback callback : callbacks) {
                callback.term(term, stats, group);
            }
        }

        @Override
        public boolean needSorted() {
            return Arrays.stream(callbacks).anyMatch(Session.StringIterateCallback::needSorted);
        }

        @Override
        public boolean needGroup() {
            return Arrays.stream(callbacks).anyMatch(Session.StringIterateCallback::needGroup);
        }

        @Override
        public boolean needStats() {
            return Arrays.stream(callbacks).anyMatch(Session.StringIterateCallback::needStats);
        }
    } 
}
