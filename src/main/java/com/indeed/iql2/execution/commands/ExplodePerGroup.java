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
import com.indeed.iql.marshal.ImhotepMarshallerInIQL.FieldOptions;
import com.indeed.iql.marshal.ImhotepMarshallerInIQL.SingleFieldMultiRemapRule;
import com.indeed.iql2.execution.Commands;
import com.indeed.iql2.execution.Session;
import java.util.function.Consumer;;
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

public class ExplodePerGroup implements Command {
    public final List<Commands.TermsWithExplodeOpts> termsWithExplodeOpts;
    private final String field;
    private final boolean isIntType;

    // all Terms inside termsWithExplodeOpts must correspond to same field.
    public ExplodePerGroup(
            final List<Commands.TermsWithExplodeOpts> termsWithExplodeOpts,
            final String field,
            final boolean isIntType) {
        this.termsWithExplodeOpts = termsWithExplodeOpts;
        this.field = field;
        this.isIntType = isIntType;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        checkNumGroups(session);

        session.timer.push("form rules");
        final SingleFieldMultiRemapRule[] rules = new SingleFieldMultiRemapRule[session.numGroups];
        int nextGroup = 1;
        final List<GroupKey> nextGroupKeys = Lists.newArrayList((GroupKey) null);
        final IntList nextGroupParents = new IntArrayList();
        nextGroupParents.add(-1);

        final Map<String, GroupKey> stringTermGroupKeys = new HashMap<>();

        final Map<String, GroupKey> defaultGroupKeys = new HashMap<>(); // Probably shared.

        for (int group = 1; group <= session.numGroups; group++) {
            final Commands.TermsWithExplodeOpts termsWithExplodeOpts = this.termsWithExplodeOpts.get(group);

            final List<Term> terms = termsWithExplodeOpts.terms;
            if (terms.isEmpty() && !termsWithExplodeOpts.defaultName.isPresent()) {
                // TODO: don't create this dummy message.
                // count message count upfront and create only nontrivial messages
                rules[group - 1] = new SingleFieldMultiRemapRule(
                        group,
                        0,
                        new int[0],
                        isIntType ? new long[0] : null,
                        isIntType ? null : new String[0]);
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
            if (termsWithExplodeOpts.defaultName.isPresent()) {
                negativeGroup = nextGroup++;
                final String defaultName = termsWithExplodeOpts.defaultName.get();
                if (!defaultGroupKeys.containsKey(defaultName)) {
                    defaultGroupKeys.put(defaultName, DefaultGroupKey.create(defaultName));
                }
                nextGroupKeys.add(defaultGroupKeys.get(defaultName));
                nextGroupParents.add(group);
            } else {
                negativeGroup = 0;
            }
            rules[group - 1] = new SingleFieldMultiRemapRule(
                    group,
                    negativeGroup,
                    positiveGroups,
                    longTerms,
                    stringTerms);
        }
        session.timer.pop();

        final FieldOptions options = new FieldOptions(field, isIntType, false);
        session.regroupWithSingleFieldRules(rules, options, true);

        session.assumeDense(DumbGroupKeySet.create(session.groupKeySet, nextGroupParents.toIntArray(), nextGroupKeys));

        out.accept("success");
    }

    private void checkNumGroups(Session session) {
        int numGroups = 0;
        for (final Commands.TermsWithExplodeOpts termsWithExplodeOpt : this.termsWithExplodeOpts) {
            if (termsWithExplodeOpt != null) {
                numGroups += termsWithExplodeOpt.terms.size();
                if (termsWithExplodeOpt.defaultName.isPresent()) {
                    numGroups += 1;
                }
            }
        }
        session.checkGroupLimit(numGroups);
    }
}
