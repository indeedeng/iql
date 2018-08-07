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

import com.google.common.primitives.Ints;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.iql2.execution.Session;
import java.util.function.Consumer;
import com.indeed.iql2.execution.groupkeys.DefaultGroupKey;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class StringRegroupFieldIn implements Command {
    private final String field;
    private final List<String> terms;
    private final boolean withDefault;

    public StringRegroupFieldIn(String field, List<String> terms, boolean withDefault) {
        this.field = field;
        this.terms = terms;
        this.withDefault = withDefault;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        session.timer.push("form rules");
        final GroupMultiRemapMessage[] messages = new GroupMultiRemapMessage[session.numGroups];
        final int numTerms = terms.size();
        final RegroupConditionMessage[] conditions = new RegroupConditionMessage[numTerms];
        final RegroupConditionMessage.Builder conditionBuilder = RegroupConditionMessage.newBuilder()
                .setField(field)
                .setIntType(false)
                .setIntTerm(0L)
                .setInequality(false);
        for (int i = 0; i < conditions.length; i++) {
            conditions[i] = conditionBuilder.setStringTerm(terms.get(i)).build();
        }
        for (int group = 1; group <= session.numGroups; group++) {
            final int[] positiveGroups = new int[numTerms];
            final int baseGroup = 1 + (group - 1) * (numTerms + (withDefault ? 1 : 0));
            for (int i = 0; i < numTerms; i++) {
                positiveGroups[i] = baseGroup + i;
            }
            final int negativeGroup = withDefault ? (baseGroup + numTerms) : 0;
            messages[group - 1] = GroupMultiRemapMessage.newBuilder()
                    .setTargetGroup(group)
                    .setNegativeGroup(negativeGroup)
                    .addAllPositiveGroup(Ints.asList(positiveGroups))
                    .addAllCondition(Arrays.asList(conditions))
                    .build();
        }
        session.timer.pop();

        session.regroupWithProtos(messages, true);

        session.densify(new StringFieldInGroupKeySet(session.groupKeySet, terms, withDefault));
    }

    public static class StringFieldInGroupKeySet implements GroupKeySet {
        private final GroupKeySet previous;
        private final List<String> terms;
        private final boolean withDefault;
        private final StringGroupKey[] groupKeys;

        public StringFieldInGroupKeySet(GroupKeySet previous, List<String> terms, boolean withDefault) {
            this.previous = previous;
            this.terms = terms;
            this.withDefault = withDefault;
            this.groupKeys = new StringGroupKey[terms.size()];
            for (int i = 0; i < terms.size(); i++) {
                this.groupKeys[i] = new StringGroupKey(terms.get(i));
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StringFieldInGroupKeySet that = (StringFieldInGroupKeySet) o;
            return withDefault == that.withDefault &&
                    Objects.equals(previous, that.previous) &&
                    Objects.equals(terms, that.terms) &&
                    Arrays.equals(groupKeys, that.groupKeys);
        }

        @Override
        public int hashCode() {
            return Objects.hash(previous, terms, withDefault, groupKeys);
        }
    }
}