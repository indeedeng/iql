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
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;

import java.util.Arrays;
import java.util.Set;

public class StringOrAction implements Action {
    public final FieldSet field;
    public final ImmutableSet<String> terms;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public StringOrAction(FieldSet field, Set<String> terms, int targetGroup, int positiveGroup, int negativeGroup) {
        this.field = field;
        this.terms = ImmutableSet.copyOf(terms);
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(final Session session) throws ImhotepOutOfMemoryException {
        session.timer.push("sort terms");
        final String[] termsArr = terms.toArray(new String[terms.size()]);
        Arrays.sort(termsArr);
        session.timer.pop();

        session.stringOrRegroup(field, termsArr, targetGroup, negativeGroup, positiveGroup);
    }

    @Override
    public String toString() {
        return "StringOrAction{" +
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
