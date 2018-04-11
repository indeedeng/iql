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
 package com.indeed.imhotep.iql;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class IntInCondition implements Condition {
    private static final Logger log = Logger.getLogger(IntInCondition.class);

    private final Field.IntField intField;
    private final long[] values;
    private final boolean negation;

    public IntInCondition(Field.IntField intField, boolean negation, long... values) {
        this.intField = intField;
        this.negation = negation;
        this.values = values;
    }

    public void filter(final EZImhotepSession session) throws ImhotepOutOfMemoryException {
        if(negation) {
            session.filterNegation(intField, values);
        } else {
            session.filter(intField, values);
        }
    }
}
