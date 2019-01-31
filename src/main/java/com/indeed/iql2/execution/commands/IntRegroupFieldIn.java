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

package com.indeed.iql2.execution.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.SingleFieldRegroupTools;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.groupkeys.DefaultGroupKey;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Arrays;
import java.util.Objects;

@Data
public class IntRegroupFieldIn implements Command {
    private final FieldSet field;
    private final LongList intTerms;
    private final boolean withDefault;

    @Override
    public void execute(final Session session) throws ImhotepOutOfMemoryException {
        session.timer.push("form rules");
        final SingleFieldRegroupTools.SingleFieldRulesBuilder rules = session.createRuleBuilder(field, true, false);
        final int numTerms = intTerms.size();
        final long[] terms = new long[numTerms];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = intTerms.getLong(i);
        }
        for (int group = 1; group <= session.numGroups; group++) {
            final int[] positiveGroups = new int[numTerms];
            final int baseGroup = 1 + (group - 1) * (numTerms + (withDefault ? 1 : 0));
            for (int i = 0; i < numTerms; i++) {
                positiveGroups[i] = baseGroup + i;
            }
            final int negativeGroup = withDefault ? (baseGroup + numTerms) : 0;
            rules.addIntRule(
                    group,
                    negativeGroup,
                    positiveGroups,
                    terms);
        }
        session.timer.pop();

        session.regroupWithSingleFieldRules(rules, field, true, false, true);

        session.densify(new IntFieldInGroupKeySet(session.groupKeySet, intTerms, withDefault));
    }

    @EqualsAndHashCode
    @ToString
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
                return DefaultGroupKey.DEFAULT_INSTANCE;
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
    }
}
