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

package com.indeed.iql2.execution;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.local.ImhotepJavaLocalSession;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.util.*;

/**
 * @author jwolfe
 */

public class Document {
    public final String dataset;
    public final long timestamp;
    public final ImmutableMap<String, ImmutableList<String>> stringFields;
    public final ImmutableMap<String, ImmutableList<Long>> intFields;

    public Document(
            final String dataset,
            final long timestamp,
            final Map<String, List<String>> stringFields,
            final Map<String, List<Long>> intFields
    ) {
        this.dataset = dataset;
        this.timestamp = timestamp;
        this.stringFields = copy(stringFields);
        this.intFields = copy(intFields);
    }

    private static <T> ImmutableMap<String, ImmutableList<T>> copy(final Map<String, List<T>> map) {
        final ImmutableMap.Builder<String, ImmutableList<T>> builder = ImmutableMap.builder();
        for (final Map.Entry<String, List<T>> entry : map.entrySet()) {
            builder.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        return builder.build();
    }

    public List<String> getStringField(final String field) {
        return stringFields.containsKey(field) ? stringFields.get(field) : Collections.<String>emptyList();
    }

    public List<Long> getIntField(final String field) {
        return intFields.containsKey(field) ? intFields.get(field) : Collections.<Long>emptyList();
    }

    // TODO: This is probably stupid expensive, but who cares?
    public long computeMetric(final List<String> metric) {
        try (final ImhotepSession session = makeImhotepSession()) {
            final int numStats = session.pushStats(metric);
            if (numStats != 1) {
                throw new IllegalArgumentException("Not exactly one metric pushed: numStats = [" + numStats + "]");
            }
            return session.getGroupStats(0)[1];
        } catch (final ImhotepOutOfMemoryException e) {
            throw Throwables.propagate(e);
        }
    }

    // TODO: This is probably stupid expensive, but who cares?
    public boolean queryMatches(final Query query) {
        try (final ImhotepSession session = makeImhotepSession()) {
            session.regroup(new QueryRemapRule(1, query, 0, 1));
            session.pushStat("count()");
            final long[] groupStats = session.getGroupStats(0);
            return groupStats.length == 2 && groupStats[1] == 1;
        } catch (final ImhotepOutOfMemoryException e) {
            throw Throwables.propagate(e);
        }
    }

    public boolean hasTerm(final Term term) {
        if (term.isIntField()) {
            return this.getIntField(term.getFieldName()).contains(term.getTermIntVal());
        } else {
            return this.getStringField(term.getFieldName()).contains(term.getTermStringVal());
        }
    }

    public ImhotepSession makeImhotepSession() {
        final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();
        memoryFlamdex.addDocument(asFlamdex());
        try {
            return new ImhotepJavaLocalSession("TestSession", memoryFlamdex);
        } catch (final ImhotepOutOfMemoryException e) {
            throw Throwables.propagate(e);
        }
    }

    public FlamdexDocument asFlamdex() {
        final Map<String, LongList> intFieldsCopy = new HashMap<>();
        for (final Map.Entry<String, ImmutableList<Long>> entry : intFields.entrySet()) {
            intFieldsCopy.put(entry.getKey(), new LongArrayList(entry.getValue()));
        }
        intFieldsCopy.put("unixtime", new LongArrayList(Collections.singletonList(timestamp / 1000)));
        final Map<String, List<String>> stringFieldsCopy = new HashMap<>();
        for (final Map.Entry<String, ImmutableList<String>> entry : stringFields.entrySet()) {
            stringFieldsCopy.put(entry.getKey(), entry.getValue());
        }
        return new FlamdexDocument(intFieldsCopy, stringFieldsCopy);
    }

    public static Document.Builder builder(String dataset, long timestamp) {
        return new Builder(dataset, timestamp);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Document document = (Document) o;
        return Objects.equals(timestamp, document.timestamp) &&
                Objects.equals(dataset, document.dataset) &&
                Objects.equals(stringFields, document.stringFields) &&
                Objects.equals(intFields, document.intFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataset, timestamp, stringFields, intFields);
    }

    public static class Builder {
        private final String dataset;
        private final long timestamp;
        private final Map<String, List<String>> stringFields = Maps.newHashMap();
        private final Map<String, List<Long>> intFields = Maps.newHashMap();

        private Builder(final String dataset, final long timestamp) {
            this.dataset = dataset;
            this.timestamp = timestamp;
        }

        public Document build() {
            return new Document(dataset, timestamp, stringFields, intFields);
        }

        public Builder addTerm(String field, String term) {
            List<String> termList = stringFields.get(field);
            if (termList == null) {
                termList = new ArrayList<>();
                stringFields.put(field, termList);
            }
            termList.add(term);
            return this;
        }

        public Builder addTerm(String field, long term) {
            List<Long> termList = intFields.get(field);
            if (termList == null) {
                termList = new ArrayList<>();
                intFields.put(field, termList);
            }
            termList.add(term);
            return this;
        }
    }
}