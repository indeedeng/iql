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
import com.indeed.iql2.Formatter;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.groupkeys.DefaultGroupKey;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@EqualsAndHashCode
@ToString
public class StringRegroupFieldIn implements Command {
    private final FieldSet field;
    private final List<String> terms;
    private final boolean withDefault;

    public StringRegroupFieldIn(final FieldSet field, final List<String> terms, final boolean withDefault) {
        this.field = field;
        this.terms = terms;
        this.withDefault = withDefault;
    }

    @Override
    public void execute(final Session session) throws ImhotepOutOfMemoryException {
        session.timer.push("form rules");
        final SingleFieldRegroupTools.SingleFieldRulesBuilder rules = session.createRuleBuilder(field, false, false);
        final int numTerms = terms.size();
        final String[] termsArray = terms.toArray(new String[0]) ;
        for (int group = 1; group <= session.numGroups; group++) {
            final int[] positiveGroups = new int[numTerms];
            final int baseGroup = 1 + (group - 1) * (numTerms + (withDefault ? 1 : 0));
            for (int i = 0; i < numTerms; i++) {
                positiveGroups[i] = baseGroup + i;
            }
            final int negativeGroup = withDefault ? (baseGroup + numTerms) : 0;
            rules.addStringRule(
                    group,
                    negativeGroup,
                    positiveGroups,
                    termsArray);
        }
        session.timer.pop();

        session.regroupWithSingleFieldRules(rules, field, false, false, true);

        session.densify(new StringFieldInGroupKeySet(session.groupKeySet, terms, withDefault, session.formatter));
    }

    @EqualsAndHashCode
    @ToString
    public static class StringFieldInGroupKeySet implements GroupKeySet {
        private final GroupKeySet previous;
        private final List<String> terms;
        private final boolean withDefault;
        private final StringGroupKey[] groupKeys;

        public StringFieldInGroupKeySet(GroupKeySet previous, List<String> terms, boolean withDefault, final Formatter formatter) {
            this.previous = previous;
            this.terms = terms;
            this.withDefault = withDefault;
            this.groupKeys = new StringGroupKey[terms.size()];
            for (int i = 0; i < terms.size(); i++) {
                this.groupKeys[i] = StringGroupKey.fromTerm(terms.get(i), formatter);
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
            if (termIndex == groupKeys.length) {
                return DefaultGroupKey.DEFAULT_INSTANCE;
            }
            return groupKeys[(termIndex)];
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