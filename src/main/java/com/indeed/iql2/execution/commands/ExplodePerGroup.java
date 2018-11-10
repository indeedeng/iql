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

import com.google.common.collect.Lists;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.SingleFieldRegroupTools;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.groupkeys.DefaultGroupKey;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExplodePerGroup implements Command {
    public final List<List<Term>> termsPerGroup;
    public final Optional<String> defaultName;

    private final String field;
    private final boolean isIntType;

    // all Terms inside termsWithExplodeOpts must correspond to same field.
    public ExplodePerGroup(
            final List<List<Term>> termsPerGroup,
            final String field,
            final boolean isIntType,
            final Optional<String> defaultName) {
        this.termsPerGroup = termsPerGroup;
        this.field = field;
        this.isIntType = isIntType;
        this.defaultName = defaultName;
    }

    @Override
    public void execute(final Session session) throws ImhotepOutOfMemoryException {
        checkNumGroups(session);

        session.timer.push("form rules");
        final SingleFieldRegroupTools.SingleFieldRulesBuilder ruleBuilder = session.createRuleBuilder(field, isIntType, false);
        int nextGroup = 1;
        final List<GroupKey> nextGroupKeys = Lists.newArrayList((GroupKey) null);
        final IntList nextGroupParents = new IntArrayList();
        nextGroupParents.add(-1);

        final Map<String, GroupKey> stringTermGroupKeys = new HashMap<>();

        final Map<String, GroupKey> defaultGroupKeys = new HashMap<>(); // Probably shared.

        for (int group = 1; group <= session.numGroups; group++) {
            final List<Term> terms = this.termsPerGroup.get(group);

            if (terms.isEmpty() && !defaultName.isPresent()) {
                continue;
            }

            final int termsCount = terms.size();
            final long[] longTerms = isIntType ? new long[termsCount] : null;
            final String[] stringTerms = isIntType ? null : new String[termsCount];
            for (int termIndex = 0; termIndex < termsCount; termIndex++) {
                final Term term = terms.get(termIndex);
                if ((term.isIntField() != isIntType) || !field.equals(term.getFieldName())) {
                    throw new IllegalStateException();
                }
                if (isIntType) {
                    final long longTerm = term.getTermIntVal();
                    longTerms[termIndex] = longTerm;
                    nextGroupKeys.add(new IntTermGroupKey(longTerm));
                    nextGroupParents.add(group);
                } else {
                    final String stringTerm = term.getTermStringVal();
                    stringTerms[termIndex] = stringTerm;
                    if (!stringTermGroupKeys.containsKey(stringTerm)) {
                        stringTermGroupKeys.put(term.getTermStringVal(), new StringGroupKey(stringTerm));
                    }
                    nextGroupKeys.add(stringTermGroupKeys.get(stringTerm));
                    nextGroupParents.add(group);
                }
            }

            final int[] positiveGroups = new int[termsCount];
            for (int j = 0; j < positiveGroups.length; j++) {
                positiveGroups[j] = nextGroup++;
            }
            final int negativeGroup;
            if (defaultName.isPresent()) {
                negativeGroup = nextGroup++;
                final String name = defaultName.get();
                if (!defaultGroupKeys.containsKey(name)) {
                    defaultGroupKeys.put(name, DefaultGroupKey.create(name));
                }
                nextGroupKeys.add(defaultGroupKeys.get(name));
                nextGroupParents.add(group);
            } else {
                negativeGroup = 0;
            }
            if (isIntType) {
                ruleBuilder.addIntRule(group, negativeGroup, positiveGroups, longTerms);
            } else {
                ruleBuilder.addStringRule(group, negativeGroup, positiveGroups, stringTerms);
            }
        }
        session.timer.pop();

        final SingleFieldRegroupTools.FieldOptions options = new SingleFieldRegroupTools.FieldOptions(field, isIntType, false);
        session.regroupWithSingleFieldRules(ruleBuilder, options, true);

        session.assumeDense(DumbGroupKeySet.create(session.groupKeySet, nextGroupParents.toIntArray(), nextGroupKeys));
    }

    private void checkNumGroups(final Session session) {
        int numGroups = 0;
        for (final List<Term> terms : this.termsPerGroup) {
            if (terms != null) {
                numGroups += terms.size();
            }
        }
        if (defaultName.isPresent()) {
            numGroups += termsPerGroup.size();
        }
        session.checkGroupLimit(numGroups);
    }
}
