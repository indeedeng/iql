package com.indeed.imhotep.iql;

import com.google.common.base.Function;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;
import com.indeed.imhotep.ez.GroupKey;
import org.apache.log4j.Logger;

import java.util.Map;

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

    public Map<Integer, GroupKey> regroup(final EZImhotepSession session, final Map<Integer, GroupKey> groupKeys) {
        throw new UnsupportedOperationException();
    }
}
