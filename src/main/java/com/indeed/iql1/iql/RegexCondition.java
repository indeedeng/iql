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
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class RegexCondition implements Condition {
    private static final Logger log = Logger.getLogger(RegexCondition.class);

    private final Field.StringField stringField;
    private final String regex;
    private final boolean negation;

    public RegexCondition(Field.StringField stringField, String regex, boolean negation) {
        this.stringField = stringField;
        this.regex = regex;
        this.negation = negation;
    }

    public void filter(final EZImhotepSession session) throws ImhotepOutOfMemoryException {
        if(negation) {
            session.filterRegexNegation(stringField, regex);
        } else {
            session.filterRegex(stringField, regex);
        }
    }
}
