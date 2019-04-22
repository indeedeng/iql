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
 package com.indeed.iql1.iql;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql1.ez.EZImhotepSession;
import com.indeed.iql1.ez.Field;

/**
 * Filters out random terms for the specified field.
 * @author vladimir
 */
public class SampleCondition implements Condition {
    public final Field field;
    public final long numerator;
    public final long denominator;
    public final String salt;

    /**
     * @param field field to sample by
     * @param numerator / @param denominator ratio of terms to remove. In the range [0,1]
     * @param salt the salt to use for hashing. Providing a constant salt will lead to a reproducible result.
     */
    public SampleCondition(final Field field, final long numerator, final long denominator, final String salt, final boolean negation) {
        this.field = field;
        this.numerator = negation ? numerator : (denominator - numerator);
        this.denominator = denominator;
        this.salt = salt;
    }

    @Override
    public void filter(EZImhotepSession session) throws ImhotepOutOfMemoryException {
        final double p = ((double)numerator) / denominator;
        session.filterSample(field, p, salt);
    }
}
