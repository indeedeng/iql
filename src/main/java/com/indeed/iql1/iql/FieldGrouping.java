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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.exceptions.GroupLimitExceededException;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.web.Limits;
import com.indeed.iql1.ez.EZImhotepSession;
import com.indeed.iql1.ez.Field;
import com.indeed.iql1.ez.GroupKey;
import com.indeed.iql1.ez.StatReference;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.indeed.iql1.ez.Stats.Stat;

/**
 * @author jplaisance
 */
public final class FieldGrouping extends Grouping {
    private static final Logger log = Logger.getLogger(FieldGrouping.class);

    private static final Stat DEFAULT_SORT_STAT = EZImhotepSession.counts();

    private final Field field;
    private final int topK;
    private final Stat sortStat;
    private final boolean isBottom;
    private final ArrayList<String> termSubset;
    private final Limits limits;
    private final int rowLimit;

    public FieldGrouping(final Field field, int rowLimit, Limits limits) {
        this(field, 0, DEFAULT_SORT_STAT, false, Collections.<String>emptyList(), rowLimit, limits);
    }

    public FieldGrouping(final Field field, List<String> termSubset, Limits limits) {
        this(field, 0, DEFAULT_SORT_STAT, false, termSubset, 0, limits);
    }

    public FieldGrouping(final Field field, int topK, Stat sortStat, boolean isBottom, Limits limits) {
        this(field, topK, sortStat, isBottom, Collections.<String>emptyList(), 0, limits);
    }

    private FieldGrouping(final Field field, int topK, Stat sortStat, boolean isBottom, List<String> termSubset, int rowLimit, Limits limits) {
        this.field = field;
        this.topK = topK;
        this.sortStat = sortStat;
        this.isBottom = isBottom;
        this.rowLimit = rowLimit;
        // remove duplicated terms as it makes Imhotep complain
        this.termSubset = Lists.newArrayList(Sets.newLinkedHashSet(termSubset));
        this.limits = limits;
        // validation
        if(!limits.satisfiesQueryInMemoryRowsLimit(topK)) {
            DecimalFormat df = new DecimalFormat("###,###");
            throw new GroupLimitExceededException("Number of requested top terms (" + df.format(topK) + ") for field " +
                    field.getFieldName() + " exceeds the limit (" + df.format(limits.queryInMemoryRowsLimit) +
                    "). Please simplify the query.");
        }
    }

    @Override
    public Int2ObjectMap<GroupKey> regroup(final EZImhotepSession session, final Int2ObjectMap<GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        if(groupKeys.isEmpty()) {
            return groupKeys;
        }
        if (isTopK()) {
            return Preconditions.checkNotNull(session.splitAllTopK(field, groupKeys, topK, sortStat, isBottom));
        } else if(isTermSubset()) {
            if(field.isIntField()) {
                Field.IntField intField = (Field.IntField) field;
                long[] termsArray = new long[termSubset.size()];
                for(int i = 0; i < termSubset.size(); i++) {
                    try {
                        termsArray[i] = Long.valueOf(termSubset.get(i));
                    } catch (NumberFormatException e) {
                        throw new IqlKnownException.ParseErrorException("IN grouping for int field " + intField.getFieldName() +
                                " has a non integer argument: " + termSubset.get(i));
                    }
                }
                return Preconditions.checkNotNull(session.explodeEachGroup(intField, termsArray, groupKeys));
            } else {
                String[] termsArray = termSubset.toArray(new String[termSubset.size()]);
                return Preconditions.checkNotNull(session.explodeEachGroup((Field.StringField) field, termsArray, groupKeys));
            }
        } else {
            return Preconditions.checkNotNull(session.splitAll(field, groupKeys, rowLimit));
        }
    }

    @Override
    public Iterator<GroupStats> getGroupStats(final EZImhotepSession session, final Int2ObjectMap<GroupKey> groupKeys, final List<StatReference> statRefs) throws ImhotepOutOfMemoryException {
        if(groupKeys.isEmpty()) {   // we don't have any parent groups probably because all docs were filtered out
            return Collections.<GroupStats>emptyList().iterator();  // so no point doing FTGS
        }
        if (isTopK()) {
            //TODO have some way of not potentially pushing counts() twice
            final StatReference countStat = session.pushStatGeneric(sortStat);
            final TopKGroupingFTGSCallback callback = new TopKGroupingFTGSCallback(session.getStackDepth(), topK, countStat, statRefs, groupKeys, isBottom, limits);
            session.ftgsIterate(field, callback);
            return callback.getResults().iterator();
        } else {
            final GroupingFTGSCallbackNoExplode callback = new GroupingFTGSCallbackNoExplode(session.getStackDepth(), statRefs, groupKeys);
            if(!isTermSubset()) {
                return session.ftgsGetIterator(field, callback, rowLimit);
            } else {
                return session.ftgsGetSubsetIterator(field, termSubset, callback);
            }
        }
    }

    public Field getField() {
        return field;
    }

    public boolean isTopK() {
        return topK != 0;
    }

    private boolean isTermSubset() {
        return !termSubset.isEmpty();
    }
}
