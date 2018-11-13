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
import com.indeed.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExplodePerGroup implements Command {
    final LongArrayList[] intTerms;
    final List<String>[] strTerms;
    public final Optional<String> defaultName;

    private final String field;
    private final boolean isIntType;

    public ExplodePerGroup(
            final String field,
            final boolean isIntType,
            final LongArrayList[] intTerms,
            final List<String>[] strTerms,
            final Optional<String> defaultName) {
        this.intTerms = intTerms;
        this.strTerms = strTerms;
        this.field = field;
        this.isIntType = isIntType;
        this.defaultName = defaultName;
    }

    @Override
    public void execute(final Session session) throws ImhotepOutOfMemoryException {
        final int resultingGroups = getGroupsCount();
        session.checkGroupLimit(resultingGroups);

        session.timer.push("form rules");
        final SingleFieldRegroupTools.SingleFieldRulesBuilder ruleBuilder = session.createRuleBuilder(field, isIntType, false);
        int nextGroup = 1;
        final List<GroupKey> nextGroupKeys = new ArrayList<>(resultingGroups + 1);
        nextGroupKeys.add(null);
        final IntList nextGroupParents = new IntArrayList(resultingGroups + 1);
        nextGroupParents.add(-1);

        final Map<String, GroupKey> stringTermGroupKeys = new HashMap<>();

        final Map<String, GroupKey> defaultGroupKeys = new HashMap<>(); // Probably shared.

        final long[] emptyLongArryy = new long[0];
        final String[] emptyStringArray = new String[0];

        for (int group = 1; group <= session.numGroups; group++) {
            final long[] longTerms = (isIntType && intTerms[group] != null) ? intTerms[group].toLongArray() : emptyLongArryy;
            final String[] stringTerms = (!isIntType && strTerms[group] != null) ? strTerms[group].toArray(emptyStringArray) : emptyStringArray;
            final int termsCount = isIntType ? longTerms.length : stringTerms.length;

            if ((termsCount == 0) && !defaultName.isPresent()) {
                continue;
            }

            for (int termIndex = 0; termIndex < termsCount; termIndex++) {
                if (isIntType) {
                    final long longTerm = longTerms[termIndex];
                    nextGroupKeys.add(new IntTermGroupKey(longTerm));
                } else {
                    final String stringTerm = stringTerms[termIndex];
                    if (!stringTermGroupKeys.containsKey(stringTerm)) {
                        stringTermGroupKeys.put(stringTerm, new StringGroupKey(stringTerm));
                    }
                    nextGroupKeys.add(stringTermGroupKeys.get(stringTerm));
                }
                nextGroupParents.add(group);
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

    private int getGroupsCount() {
        int numGroups = 0;
        if (isIntType) {
            for (final LongArrayList terms : intTerms) {
                if (terms != null) {
                    numGroups += terms.size();
                }
            }
            if (defaultName.isPresent()) {
                numGroups += intTerms.length;
            }
        } else {
            for (final List<String> terms : strTerms) {
                if (terms != null) {
                    numGroups += terms.size();
                }
            }
            if (defaultName.isPresent()) {
                numGroups += strTerms.length;
            }
        }
        return numGroups;
    }
}
