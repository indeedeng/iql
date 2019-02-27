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

package com.indeed.iql2.execution.actions;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class IntOrAction implements Action {
    public final FieldSet field;
    public final ImmutableSet<Long> terms;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    @Nullable
    private Set<String> stringifiedTerms;

    public IntOrAction(FieldSet field, Set<Long> terms, int targetGroup, int positiveGroup, int negativeGroup) {
        this.field = field;
        this.terms = ImmutableSet.copyOf(terms);
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(Session session) throws ImhotepOutOfMemoryException {
        for (final Map.Entry<String, Session.ImhotepSessionInfo> e : session.sessions.entrySet()) {
            final String dataset = e.getKey();
            if (field.containsDataset(dataset)) {
                final String fieldName = field.datasetFieldName(dataset);
                final Session.ImhotepSessionInfo sessionInfo = e.getValue();
                if (!sessionInfo.intFields.contains(fieldName)) {
                    new StringOrAction(field.subset(Collections.singleton(dataset)), stringifiedTerms(), targetGroup, positiveGroup, negativeGroup).apply(session);
                } else {
                    session.timer.push("sort terms");
                    final long[] terms = new long[this.terms.size()];
                    int i = 0;
                    for (final long term : this.terms) {
                        terms[i] = term;
                        i++;
                    }
                    Arrays.sort(terms);
                    session.timer.pop();
                    session.intOrRegroup(fieldName, terms, targetGroup, negativeGroup, positiveGroup, Collections.singleton(dataset));
                }
            }
        }
    }

    private Set<String> stringifiedTerms() {
        if (stringifiedTerms == null) {
            stringifiedTerms = Sets.newHashSetWithExpectedSize(terms.size());
            for (final long term : terms) {
                stringifiedTerms.add(String.valueOf(term));
            }
        }
        return stringifiedTerms;
    }

    @Override
    public String toString() {
        return "IntOrAction{" +
                "field=" + field +
                ", terms=" + renderTerms() +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }

    private String renderTerms() {
        if (terms.size() <= 10) {
            return terms.toString();
        } else {
            return "(" + terms.size() + " terms)";
        }
    }
}
