package com.indeed.squall.iql2.execution.commands;

import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.DefaultGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class IntRegroupFieldIn implements Command {
    private final String field;
    private final LongList intTerms;
    private final boolean withDefault;

    public IntRegroupFieldIn(String field, LongList intTerms, boolean withDefault) {
        this.field = field;
        this.intTerms = intTerms;
        this.withDefault = withDefault;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        session.timer.push("form rules");
        final GroupMultiRemapRule[] remapRules = new GroupMultiRemapRule[session.numGroups];
        final int numTerms = intTerms.size();
        final RegroupCondition[] conditions = new RegroupCondition[numTerms];
        for (int i = 0; i < conditions.length; i++) {
            conditions[i] = new RegroupCondition(field, true, intTerms.getLong(i), null, false);
        }
        for (int group = 1; group <= session.numGroups; group++) {
            final int[] positiveGroups = new int[numTerms];
            final int baseGroup = 1 + (group - 1) * (numTerms + (withDefault ? 1 : 0));
            for (int i = 0; i < numTerms; i++) {
                positiveGroups[i] = baseGroup + i;
            }
            final int negativeGroup = withDefault ? (baseGroup + numTerms) : 0;
            remapRules[group - 1] = new GroupMultiRemapRule(group, negativeGroup, positiveGroups, conditions);
        }
        session.timer.pop();

        session.regroup(remapRules, false);

        session.densify(new IntFieldInGroupKeySet(session.groupKeySet, intTerms, withDefault));
        session.currentDepth += 1;
    }

    public static class IntFieldInGroupKeySet implements GroupKeySet {
        private final GroupKeySet previous;
        private final LongList terms;
        private final boolean withDefault;
        private final IntTermGroupKey[] groupKeys;

        public IntFieldInGroupKeySet(GroupKeySet previous, LongList terms, boolean withDefault) {
            this.previous = previous;
            this.terms = terms;
            this.withDefault = withDefault;
            this.groupKeys = new IntTermGroupKey[terms.size()];
            for (int i = 0; i < terms.size(); i++) {
                this.groupKeys[i] = new IntTermGroupKey(terms.getLong(i));
            }
        }

        private int groupsPerOldGroup() {
            return terms.size() + (withDefault ? 1 : 0);
        }

        @Override
        public GroupKeySet previous() {
            return previous;
        }

        @Override
        public int parentGroup(int group) {
            return 1 + (group - 1) / groupsPerOldGroup();
        }

        @Override
        public GroupKey groupKey(int group) {
            final int termIndex = (group - 1) % groupsPerOldGroup();
            if (termIndex == this.groupKeys.length) {
                return DefaultGroupKey.INSTANCE;
            }
            return this.groupKeys[(termIndex)];
        }

        @Override
        public int numGroups() {
            return previous.numGroups() * groupsPerOldGroup();
        }

        @Override
        public boolean isPresent(int group) {
            return group > 0 && group <= numGroups() && previous.isPresent(parentGroup(group));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IntFieldInGroupKeySet that = (IntFieldInGroupKeySet) o;
            return withDefault == that.withDefault &&
                    Objects.equals(previous, that.previous) &&
                    Objects.equals(terms, that.terms) &&
                    Arrays.equals(groupKeys, that.groupKeys);
        }

        @Override
        public int hashCode() {
            return Objects.hash(previous, terms, withDefault, groupKeys);
        }
    }
}
