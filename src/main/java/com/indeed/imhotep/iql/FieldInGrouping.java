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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;
import com.indeed.imhotep.ez.GroupKey;
import com.indeed.imhotep.ez.StatReference;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.indeed.imhotep.ez.Stats.Stat;

/**
 * @author jplaisance
 */
public final class FieldInGrouping extends Grouping {
    private static final Logger log = Logger.getLogger(FieldInGrouping.class);

    private final Field field;
    private final List<String> terms;

    public Field getField() {
        return field;
    }

    public FieldInGrouping(Field field, List<String> terms) {
        this.field = field;
        // remove duplicated terms as it makes Imhotep complain
        this.terms = Lists.newArrayList(Sets.newLinkedHashSet(terms));
    }

    public Map<Integer, GroupKey> regroup(final EZImhotepSession session, final Map<Integer, GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        if(field.isIntField()) {
            Field.IntField intField = (Field.IntField) field;
            long[] termsArray = new long[terms.size()];
            for(int i = 0; i < terms.size(); i++) {
                try {
                    termsArray[i] = Long.valueOf(terms.get(i));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("IN grouping for int field " + intField.getFieldName() +
                            " has a non integer argument: " + terms.get(i));
                }
            }
            return Preconditions.checkNotNull(session.explodeEachGroup(intField, termsArray, groupKeys));
        } else {
            String[] termsArray = terms.toArray(new String[terms.size()]);
            return Preconditions.checkNotNull(session.explodeEachGroup((Field.StringField) field, termsArray, groupKeys));
        }
    }
}
