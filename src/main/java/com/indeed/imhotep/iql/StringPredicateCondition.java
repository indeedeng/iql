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

import com.google.common.base.Predicate;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class StringPredicateCondition implements Condition {
    private static final Logger log = Logger.getLogger(StringPredicateCondition.class);

    private final Field.StringField stringField;
    private final Predicate<String> predicate;
    private final boolean negation;

    public StringPredicateCondition(final Field.StringField stringField, final Predicate<String> predicate, final boolean negation) {
        this.stringField = stringField;
        this.predicate = predicate;
        this.negation = negation;
    }

    public void filter(final EZImhotepSession session) throws ImhotepOutOfMemoryException {
        if(negation) {
            session.filterNegation(stringField, predicate);
        } else {
            session.filter(stringField, predicate);
        }
    }
}
