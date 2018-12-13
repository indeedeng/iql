package com.indeed.iql2.execution.groupkeys.sets;

import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.iql2.execution.groupkeys.StringGroupKey;

import javax.annotation.Nullable;

public abstract class TermsGroupKeySet implements GroupKeySet {

    private final GroupKeySet previous;
    private final int termsCount;
    private final int[] parentGroups;
    @Nullable
    private final GroupKey defaultKey;
    @Nullable
    private final boolean[] isDefaultGroup;

    private TermsGroupKeySet(
            final GroupKeySet previous,
            final int termsCount,
            final int[] parentGroups,
            final GroupKey defaultKey,
            final boolean[] isDefaultGroup) {
        this.previous = previous;
        this.termsCount = termsCount;
        this.parentGroups = parentGroups;
        this.defaultKey = defaultKey;
        this.isDefaultGroup = isDefaultGroup;
    }

    @Override
    public GroupKeySet previous() {
        return previous;
    }

    @Override
    public int parentGroup(final int group) {
        return parentGroups[group];
    }

    @Override
    public GroupKey groupKey(final int group) {
        if ((isDefaultGroup != null) && isDefaultGroup[group]) {
            return defaultKey;
        }
        return getNonDefaultGroupKey(group);
    }

    @Override
    public int numGroups() {
        return termsCount - 1;
    }

    @Override
    public boolean isPresent(final int group) {
        return (group >= 0) && (group < termsCount);
    }

    protected abstract GroupKey getNonDefaultGroupKey(final int group);

    public static class IntTerms extends TermsGroupKeySet {

        private final long[] terms;

        public IntTerms(
                final GroupKeySet previous,
                final long[] terms,
                final int[] parentGroups,
                final GroupKey defaultKey,
                final boolean[] isDefaultGroup) {
            super(previous, terms.length, parentGroups, defaultKey, isDefaultGroup);
            this.terms = terms;
        }

        @Override
        protected GroupKey getNonDefaultGroupKey(final int group) {
            return new IntTermGroupKey(terms[group]);
        }
    }

    public static class StringTerms extends TermsGroupKeySet {

        private final String[] terms;

        public StringTerms(
                final GroupKeySet previous,
                final String[] terms,
                final int[] parentGroups,
                final GroupKey defaultKey,
                final boolean[] isDefaultGroup) {
            super(previous, terms.length, parentGroups, defaultKey, isDefaultGroup);
            this.terms = terms;
        }

        @Override
        protected GroupKey getNonDefaultGroupKey(final int group) {
            return new StringGroupKey(terms[group]);
        }
    }
}
