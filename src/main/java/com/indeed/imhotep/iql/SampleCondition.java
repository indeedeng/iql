/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.iql;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;

/**
 * Filters out random terms for the specified field.
 * @author vladimir
 */
public class SampleCondition implements Condition {
    private final Field field;
    private final double p;
    private final String salt;

    /**
     * @param field field to sample by
     * @param p ratio of terms to remove. In the range [0,1]
     * @param salt the salt to use for hashing. Providing a constant salt will lead to a reproducible result.
     */
    public SampleCondition(Field field, double p, String salt, boolean negation) {
        this.field = field;
        this.p = negation ? p : (1-p);
        this.salt = salt;
    }

    @Override
    public void filter(EZImhotepSession session) throws ImhotepOutOfMemoryException {
        session.filterSample(field, p, salt);
    }
}
