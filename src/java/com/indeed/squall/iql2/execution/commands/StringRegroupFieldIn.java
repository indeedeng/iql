package com.indeed.squall.iql2.execution.commands;

import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.io.IOException;
import java.util.List;

public class StringRegroupFieldIn implements Command {
    private final String field;
    private final List<String> terms;

    public StringRegroupFieldIn(String field, List<String> terms) {
        this.field = field;
        this.terms = terms;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        session.timer.push("form rules");
        final GroupMultiRemapRule[] remapRules = new GroupMultiRemapRule[session.numGroups];
        final int numTerms = terms.size();
        final RegroupCondition[] conditions = new RegroupCondition[numTerms];
        for (int i = 0; i < conditions.length; i++) {
            conditions[i] = new RegroupCondition(field, false, 0, terms.get(i), false);
        }
        for (int group = 1; group <= session.numGroups; group++) {
            final int[] positiveGroups = new int[numTerms];
            final int baseGroup = 1 + (group - 1) * numTerms;
            for (int i = 0; i < numTerms; i++) {
                positiveGroups[i] = baseGroup + i;
            }
            remapRules[group - 1] = new GroupMultiRemapRule(group, 0, positiveGroups, conditions);
        }
        session.timer.pop();

        session.regroup(remapRules, false);

        session.densify(new StringFieldInGroupKeySet(session.groupKeySet, terms));
        session.currentDepth += 1;
    }

    public static class StringFieldInGroupKeySet implements GroupKeySet {
        private final GroupKeySet previous;
        private final List<String> terms;
        private final StringGroupKey[] groupKeys;

        public StringFieldInGroupKeySet(GroupKeySet previous, List<String> terms) {
            this.previous = previous;
            this.terms = terms;
            this.groupKeys = new StringGroupKey[terms.size()];
            for (int i = 0; i < terms.size(); i++) {
                this.groupKeys[i] = new StringGroupKey(terms.get(i));
            }
        }

        @Override
        public GroupKeySet previous() {
            return previous;
        }

        @Override
        public int parentGroup(int group) {
            return 1 + (group - 1) % terms.size();
        }

        @Override
        public GroupKey groupKey(int group) {
            return this.groupKeys[(group - 1) % terms.size()];
        }

        @Override
        public int numGroups() {
            return previous.numGroups() * terms.size();
        }

        @Override
        public boolean isPresent(int group) {
            return group <= numGroups() && previous.isPresent(parentGroup(group));
        }
    }
}