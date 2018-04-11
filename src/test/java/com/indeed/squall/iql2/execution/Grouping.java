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

package com.indeed.squall.iql2.execution;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.indeed.flamdex.query.Term;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import javax.annotation.Nullable;
import java.util.*;

/**
 * @author jwolfe
 */

public class Grouping {
    @Nullable
    public final Grouping parent;

    public final ImmutableMap<Integer, ImmutableList<Document>> groupings;
    public final int[] parentGroups;

    public Grouping(final Grouping parent, final ImmutableMap<Integer, ImmutableList<Document>> groupings, final int[] parentGroups) {
        this.parent = parent;
        this.groupings = groupings;
        this.parentGroups = parentGroups;
    }

    public static Grouping from(final List<Document> documents) {
        return new Grouping(null, ImmutableMap.of(1, ImmutableList.copyOf(documents)), null);
    }

    private static Grouping from(final Grouping grouping, final Map<Integer, List<Document>> newGroupings, final int[] parentGroups) {
        return new Grouping(grouping, copy(newGroupings), parentGroups);
    }

    private static <T> ImmutableMap<Integer, ImmutableList<T>> copy(final Map<Integer, List<T>> newGroupings) {
        final ImmutableMap.Builder<Integer, ImmutableList<T>> result = ImmutableMap.builder();
        for (final Map.Entry<Integer, List<T>> entry : newGroupings.entrySet()) {
            result.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        return result.build();
    }

    public int getNumGroups() {
        if (groupings.isEmpty()) {
            return 1;
        }
        return Ordering.natural().max(groupings.keySet());
    }

    public void process(final ProcessCallback callback) {
        for (final Map.Entry<Integer, ImmutableList<Document>> entry : groupings.entrySet()) {
            for (final Document document : entry.getValue()) {
                callback.handle(entry.getKey(), document);
            }
        }
    }

    public Grouping createNew(final GroupingCallback callback) {
        final Map<Integer, List<Document>> newGroupings = new HashMap<>();

        final boolean[] didSomething = new boolean[1];
        final int[] currentGroup = new int[1];
        final Document[] currentDocument = new Document[1];

        final Map<Integer, Integer> newGroupToOldGroup = new Int2IntOpenHashMap();

        callback.addToGroup = new Function<Integer, Void>() {
            public Void apply(@Nullable final Integer input) {
                if (!newGroupings.containsKey(input)) {
                    newGroupings.put(input, new ArrayList<Document>());
                    newGroupToOldGroup.put(input, currentGroup[0]);
                }
                newGroupings.get(input).add(currentDocument[0]);
                if (newGroupToOldGroup.get(input) != currentGroup[0]) {
                    throw new IllegalStateException("Cannot have a group be formed from multiple parent groups");
                }
                didSomething[0] = true;
                return null;
            }
        };

        callback.discard = new Function<Void, Void>() {
            public Void apply(@Nullable final Void input) {
                didSomething[0] = true;
                return null;
            }
        };

        for (final Map.Entry<Integer, ImmutableList<Document>> entry : groupings.entrySet()) {
            currentGroup[0] = entry.getKey();
            for (final Document document : entry.getValue()) {
                didSomething[0] = false;
                currentDocument[0] = document;
                callback.handle(currentGroup[0], document);
                if (!didSomething[0]) {
                    throw new IllegalStateException("Failed to discard or propagate a document.");
                }
            }
        }

        return Grouping.from(this, newGroupings, parentGroups);
    }

    public List<Term> getFieldTerms(final String field) {
        return null;
    }

    public int parentGroup(final int group) {
        return parentGroups[group];
    }

    public Grouping mergeIntoParent(GroupLookupMergeType mergeType) {
        // TODO: Create new Grouping with the same doc lists but different GroupLookup stuff.
        return this.parent;
    }

    // TODO: This is super gross...
    public Grouping regroupIntoLastSibling(AggregateFilter filter, GroupLookupMergeType mergeType) {
        // TODO: GroupLookupMergeType stuff
        final Int2IntOpenHashMap parentToLastChild = new Int2IntOpenHashMap();
        for (int group = 1; group < parentGroups.length; group++) {
            parentToLastChild.put(parentGroup(group), group);
        }
        final IntOpenHashSet cascadingGroup = new IntOpenHashSet();
        final boolean[] mergeOver = this.selectFilter(Predicates.<Document>alwaysTrue(), filter);
        final Int2IntOpenHashMap groupToNewGroup = new Int2IntOpenHashMap();
        for (int group = 1; group <= getNumGroups(); group++) {
            if (mergeOver[group]) {
                cascadingGroup.add(parentGroup(group));
            }
            final int target;
            if (cascadingGroup.contains(parentGroup(group))) {
                target = parentToLastChild.get(parentGroup(group));
            } else {
                target = group;
            }
            groupToNewGroup.put(group, target);
        }
        final IntOpenHashSet newGroupsPresent = new IntOpenHashSet(groupToNewGroup.values());
        final Int2IntOpenHashMap newGroupToNewNewGroup = new Int2IntOpenHashMap();
        int nextGroup = 1;
        for (int i = 1; i <= getNumGroups(); i++) {
            if (newGroupsPresent.contains(i)) {
                newGroupToNewNewGroup.put(i, nextGroup);
                nextGroup++;
            }
        }
        return createNew(new GroupingCallback() {
            @Override
            public void handle(int group, Document document) {
                addToGroup(newGroupToNewNewGroup.get(groupToNewGroup.get(group)));
            }
        });
    }

    public void createGroupStatsLookup(double[] stats, Optional<String> name) {
        throw new UnsupportedOperationException();
    }

    private interface Stat {
        long valueOf(Document document);
    }

    private static class Stats {
        private static Stat fromPushes(final List<String> pushes) {
            return new Stat() {
                @Override
                public long valueOf(final Document document) {
                    return document.computeMetric(pushes);
                }
            };
        }
    }

    public double[][] select(final Predicate<Document> predicate, final List<AggregateMetric> selects) {
        final Set<QualifiedPush> allRequires = new HashSet<>();
        for (final AggregateMetric select : selects) {
            allRequires.addAll(select.requires());
        }
        final Map<QualifiedPush, Integer> metricIndexes = new HashMap<>();
        final long[][] docSelects = new long[allRequires.size()][];
        int index = 0;
        for (final QualifiedPush require : allRequires) {
            metricIndexes.put(require, metricIndexes.size());
            docSelects[index] = selectDoc(Predicates.and(predicate, new Predicate<Document>() {
                public boolean apply(final Document input) {
                    return input.dataset.equals(require.sessionName);
                }
            }), Stats.fromPushes(require.pushes));
            index += 1;
        }
        final int numGroups = getNumGroups();
        final double[][] result = new double[docSelects.length][];
        for (int i = 0; i < selects.size(); i++) {
            result[i] = selects.get(i).getGroupStats(docSelects, numGroups);
        }
        return result;
    }

    public boolean[] selectFilter(final Predicate<Document> hasTerm, final AggregateFilter aggregateFilter) {
        final Set<QualifiedPush> requires = aggregateFilter.requires();
        final Map<QualifiedPush, Integer> metricIndexes = new HashMap<>();
        final long[][] docSelects = new long[requires.size()][];
        int index = 0;
        for (final QualifiedPush require : requires) {
            metricIndexes.put(require, metricIndexes.size());
            docSelects[index] = selectDoc(Predicates.and(hasTerm, new Predicate<Document>() {
                @Override
                public boolean apply(final Document input) {
                    return input.dataset.equals(require.sessionName);
                }
            }), Stats.fromPushes(require.pushes));
            index += 1;
        }
        return aggregateFilter.getGroupStats(docSelects, getNumGroups());
    }

    private long[] selectDoc(final Predicate<Document> predicate, final Stat stat) {
        final long[] result = new long[getNumGroups() + 1];
        for (final Map.Entry<Integer, ImmutableList<Document>> entry : groupings.entrySet()) {
            final int group = entry.getKey();
            final ImmutableList<Document> docs = entry.getValue();
            for (final Document doc : docs) {
                if (predicate.apply(doc)) {
                    result[group] += stat.valueOf(doc);
                }
            }
        }
        return result;
    }

    // I guess this is one way to provide methods that can be called from a callback..
    public static abstract class GroupingCallback {
        private Function<Integer, Void> addToGroup;
        private Function<Void, Void> discard;

        public abstract void handle(int group, Document document);

        final void addToGroup(int group) {
            if (group == 0) {
                discard.apply(null);
            } else {
                addToGroup.apply(group);
            }
        }
        final void discard() {
            discard.apply(null);
        }
    }

    public interface ProcessCallback {
        void handle(int group, Document document);
    }
}
