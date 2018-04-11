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

import com.google.common.base.Function;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;
import com.indeed.imhotep.ez.GroupKey;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class StringMapGrouping<A> extends Grouping {
    private static final Logger log = Logger.getLogger(StringMapGrouping.class);

    private final Field.StringField stringField;
    private final Function<String, A> function;

    public StringMapGrouping(final Field.StringField stringField, final Function<String, A> function) {
        this.stringField = stringField;
        this.function = function;
    }

    public Int2ObjectMap<GroupKey> regroup(final EZImhotepSession session, final Int2ObjectMap<GroupKey> groupKeys) {
        throw new UnsupportedOperationException();
    }
}
