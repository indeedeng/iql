package com.indeed.squall.iql2.execution.commands;

import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.IOException;

public class IntRegroupFieldIn implements Command {
    private final String field;
    private final LongList intTerms;

    public IntRegroupFieldIn(String field, LongList intTerms) {
        this.field = field;
        this.intTerms = intTerms;
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
            final int baseGroup = 1 + (group - 1) * numTerms;
            for (int i = 0; i < numTerms; i++) {
                positiveGroups[i] = baseGroup + i;
            }
            remapRules[group - 1] = new GroupMultiRemapRule(group, 0, positiveGroups, conditions);
        }
        session.timer.pop();

        session.regroup(remapRules, false);

        session.densify(new IntFieldInGroupKeySet(session.groupKeySet, intTerms));
        session.currentDepth += 1;
    }

    public static class IntFieldInGroupKeySet implements GroupKeySet {
        private final GroupKeySet previous;
        private final LongList terms;
        private final IntTermGroupKey[] groupKeys;

        public IntFieldInGroupKeySet(GroupKeySet previous, LongList terms) {
            this.previous = previous;
            this.terms = terms;
            this.groupKeys = new IntTermGroupKey[terms.size()];
            for (int i = 0; i < terms.size(); i++) {
                this.groupKeys[i] = new IntTermGroupKey(terms.getLong(i));
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
