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

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;

public class SampleAction implements Action {
    public final FieldSet field;
    public final long numerator;
    public final long denominator;
    public final String seed;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public SampleAction(
            final FieldSet field,
            final long numerator,
            final long denominator,
            final String seed,
            final int targetGroup,
            final int positiveGroup,
            final int negativeGroup) {
        this.field = field;
        this.numerator = numerator;
        this.denominator = denominator;
        this.seed = seed;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(final Session session) throws ImhotepOutOfMemoryException {
        final boolean isIntField;
        if (session.isIntField(field)) {
            isIntField = true;
        } else if (session.isStringField(field)) {
            isIntField = false;
        } else {
            throw new IllegalArgumentException("field is not valid: " + field);
        }
        final double probability = ((double)numerator) / denominator;
        session.randomRegroup(field, isIntField, seed, 1.0 - probability, targetGroup, positiveGroup, negativeGroup);
    }

    @Override
    public String toString() {
        return "SampleAction{" +
                "field=" + field +
                ", numerator=" + numerator +
                ", denominator=" + denominator +
                ", seed='" + seed + '\'' +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
