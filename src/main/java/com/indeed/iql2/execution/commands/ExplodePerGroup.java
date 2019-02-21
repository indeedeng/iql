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

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.SingleFieldRegroupTools;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.groupkeys.DefaultGroupKey;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.groupkeys.sets.TermsGroupKeySet;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.List;

public class ExplodePerGroup implements Command {
    private final LongArrayList[] intTerms;
    private final List<String>[] strTerms;
    private final Optional<String> defaultName;

    private final FieldSet field;
    private final boolean isIntType;

    public ExplodePerGroup(
            final FieldSet field,
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
        final long[] nextGroupIntTerms = isIntType ? new long[resultingGroups + 1] : null;
        final String[] nextGroupStringTerms = isIntType ? null : new String[resultingGroups + 1];
        final boolean[] isDefault = defaultName.isPresent() ? new boolean[resultingGroups + 1] : null;
        final int[] nextGroupParents = new int[resultingGroups + 1];
        nextGroupParents[0] = -1;

        final long[] emptyLongArray = new long[0];
        final String[] emptyStringArray = new String[0];

        for (int group = 1; group <= session.numGroups; group++) {
            final long[] longTerms = (isIntType && intTerms[group] != null) ? intTerms[group].toLongArray() : emptyLongArray;
            final String[] stringTerms = (!isIntType && strTerms[group] != null) ? strTerms[group].toArray(emptyStringArray) : emptyStringArray;
            final int termsCount = isIntType ? longTerms.length : stringTerms.length;

            if ((termsCount == 0) && !defaultName.isPresent()) {
                continue;
            }

            if (isIntType) {
                System.arraycopy(longTerms, 0, nextGroupIntTerms, nextGroup, termsCount);
            } else {
                System.arraycopy(stringTerms, 0, nextGroupStringTerms, nextGroup, termsCount);
            }

            final int[] positiveGroups = new int[termsCount];
            for (int termIndex = 0; termIndex < termsCount; termIndex++) {
                nextGroupParents[nextGroup] = group;
                positiveGroups[termIndex] = nextGroup;
                nextGroup++;
            }

            final int negativeGroup;
            if (defaultName.isPresent()) {
                negativeGroup = nextGroup;
                isDefault[nextGroup] = true;
                nextGroupParents[nextGroup] = group;
                nextGroup++;
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

        session.regroupWithSingleFieldRules(ruleBuilder, field, isIntType, false, true);

        final GroupKeySet newKeySet;
        final GroupKey defaultKey = defaultName.isPresent() ? DefaultGroupKey.create(defaultName.get(), session.formatter) : null;
        if (isIntType) {
            newKeySet = new TermsGroupKeySet.IntTerms(session.groupKeySet, nextGroupIntTerms, nextGroupParents, defaultKey, isDefault);
        } else {
            newKeySet = new TermsGroupKeySet.StringTerms(session.groupKeySet, nextGroupStringTerms, nextGroupParents, defaultKey, isDefault, session.formatter);
        }

        session.assumeDense(newKeySet);
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
                numGroups += intTerms.length - 1; // not counting zero group;
            }
        } else {
            for (final List<String> terms : strTerms) {
                if (terms != null) {
                    numGroups += terms.size();
                }
            }
            if (defaultName.isPresent()) {
                numGroups += strTerms.length - 1; // not counting zero group;
            }
        }
        return numGroups;
    }
}
