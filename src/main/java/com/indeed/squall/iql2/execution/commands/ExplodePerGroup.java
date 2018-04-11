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

package com.indeed.squall.iql2.execution.commands;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.squall.iql2.execution.Commands;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.DefaultGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.IntTermGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExplodePerGroup implements Command {
    public final List<Commands.TermsWithExplodeOpts> termsWithExplodeOpts;

    public ExplodePerGroup(List<Commands.TermsWithExplodeOpts> termsWithExplodeOpts) {
        this.termsWithExplodeOpts = termsWithExplodeOpts;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        checkNumGroups(session);

        session.timer.push("form rules");
        final GroupMultiRemapMessage[] messages = new GroupMultiRemapMessage[session.numGroups];
        int nextGroup = 1;
        final List<GroupKey> nextGroupKeys = Lists.newArrayList((GroupKey) null);
        final IntList nextGroupParents = new IntArrayList();
        nextGroupParents.add(-1);

        final Map<String, GroupKey> stringTermGroupKeys = new HashMap<>();

        final Map<String, GroupKey> defaultGroupKeys = new HashMap<>(); // Probably shared.

        for (int group = 1; group <= session.numGroups; group++) {
            final Commands.TermsWithExplodeOpts termsWithExplodeOpts = this.termsWithExplodeOpts.get(group);

            final List<RegroupConditionMessage> regroupConditionsList = Lists.newArrayList();

            final List<Term> terms = termsWithExplodeOpts.terms;
            if (terms.isEmpty() && !termsWithExplodeOpts.defaultName.isPresent()) {
                RegroupConditionMessage fakeCondition = RegroupConditionMessage.newBuilder()
                        .setField("fakeField")
                        .setIntType(true)
                        .setIntTerm(0L)
                        .setInequality(false)
                        .build();
                messages[group - 1] = GroupMultiRemapMessage.newBuilder()
                        .setTargetGroup(group)
                        .setNegativeGroup(0)
                        .addPositiveGroup(0)
                        .addCondition(fakeCondition)
                        .build();
                continue;
            }

            for (final Term term : terms) {
                if (term.isIntField()) {
                    regroupConditionsList.add(RegroupConditionMessage.newBuilder()
                            .setField(term.getFieldName())
                            .setIntType(true)
                            .setIntTerm(term.getTermIntVal())
                            .setInequality(false)
                            .build());
                    nextGroupKeys.add(new IntTermGroupKey(term.getTermIntVal()));
                    nextGroupParents.add(group);
                } else {
                    regroupConditionsList.add(RegroupConditionMessage.newBuilder()
                            .setField(term.getFieldName())
                            .setIntType(false)
                            .setIntTerm(0)
                            .setStringTerm(term.getTermStringVal())
                            .setInequality(false)
                            .build());
                    if (!stringTermGroupKeys.containsKey(term.getTermStringVal())) {
                        stringTermGroupKeys.put(term.getTermStringVal(), new StringGroupKey(term.getTermStringVal()));
                    }
                    nextGroupKeys.add(stringTermGroupKeys.get(term.getTermStringVal()));
                    nextGroupParents.add(group);
                }
            }

            final int[] positiveGroups = new int[regroupConditionsList.size()];
            for (int j = 0; j < regroupConditionsList.size(); j++) {
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
            messages[group - 1] = GroupMultiRemapMessage.newBuilder()
                    .setTargetGroup(group)
                    .setNegativeGroup(negativeGroup)
                    .addAllPositiveGroup(Ints.asList(positiveGroups))
                    .addAllCondition(regroupConditionsList)
                    .build();
        }
        session.timer.pop();

        session.regroupWithProtos(messages, true);

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
