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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.exceptions.GroupLimitExceededException;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.ez.Field;
import com.indeed.imhotep.ez.GroupKey;
import com.indeed.imhotep.ez.StatReference;
import com.indeed.iql.web.Limits;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.indeed.imhotep.ez.Stats.Stat;

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
    private final boolean noExplode;
    private final ArrayList<String> termSubset;
    private final Limits limits;
    private final int rowLimit;

    public FieldGrouping(final Field field, final boolean noExplode, int rowLimit, Limits limits) {
        this(field, 0, DEFAULT_SORT_STAT, false, noExplode, Collections.<String>emptyList(), rowLimit, limits);
    }

    public FieldGrouping(final Field field, boolean noExplode, List<String> termSubset, Limits limits) {
        this(field, 0, DEFAULT_SORT_STAT, false, noExplode, termSubset, 0, limits);
    }

    public FieldGrouping(final Field field, int topK, Stat sortStat, boolean isBottom, Limits limits) {
        this(field, topK, sortStat, isBottom, false, Collections.<String>emptyList(), 0, limits);
    }

    public FieldGrouping(final Field field, int topK, Stat sortStat, boolean isBottom, boolean noExplode, List<String> termSubset, int rowLimit, Limits limits) {
        this.field = field;
        this.topK = topK;
        this.sortStat = sortStat;
        this.isBottom = isBottom;
        this.noExplode = noExplode;
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

    public Int2ObjectMap<GroupKey> regroup(final EZImhotepSession session, final Int2ObjectMap<GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        if(groupKeys.isEmpty()) {
            return groupKeys;
        }
        if (topK > 0) {
            return Preconditions.checkNotNull(session.splitAllTopK(field, groupKeys, topK, sortStat, isBottom));
        } else if(isTermSubset()) {
            if(field.isIntField()) {
                Field.IntField intField = (Field.IntField) field;
                long[] termsArray = new long[termSubset.size()];
                for(int i = 0; i < termSubset.size(); i++) {
                    try {
                        termsArray[i] = Long.valueOf(termSubset.get(i));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("IN grouping for int field " + intField.getFieldName() +
                                " has a non integer argument: " + termSubset.get(i));
                    }
                }
                return Preconditions.checkNotNull(session.explodeEachGroup(intField, termsArray, groupKeys));
            } else {
                String[] termsArray = termSubset.toArray(new String[termSubset.size()]);
                return Preconditions.checkNotNull(session.explodeEachGroup((Field.StringField) field, termsArray, groupKeys));
            }
        } else if(noExplode) {
            return Preconditions.checkNotNull(session.splitAll(field, groupKeys, rowLimit));
        } else {
            return Preconditions.checkNotNull(session.splitAllExplode(field, groupKeys, rowLimit));
        }
    }

    public Iterator<GroupStats> getGroupStats(final EZImhotepSession session, final Int2ObjectMap<GroupKey> groupKeys, final List<StatReference> statRefs, long timeoutTS) throws ImhotepOutOfMemoryException {
        if(groupKeys.isEmpty()) {   // we don't have any parent groups probably because all docs were filtered out
            return Collections.<GroupStats>emptyList().iterator();  // so no point doing FTGS
        }
        if (topK > 0) {
            //TODO have some way of not potentially pushing counts() twice
            final StatReference countStat = session.pushStatGeneric(sortStat);
            final TopKGroupingFTGSCallback callback = new TopKGroupingFTGSCallback(session.getStackDepth(), topK, countStat, statRefs, groupKeys, isBottom, limits);
            session.ftgsIterate(Arrays.asList(field), callback);
            return callback.getResults().iterator();
        } else if(noExplode) {
            final GroupingFTGSCallbackNoExplode callback = new GroupingFTGSCallbackNoExplode(session.getStackDepth(), statRefs, groupKeys);
            if(!isTermSubset()) {
                return session.ftgsGetIterator(Arrays.asList(field), callback, rowLimit);
            } else {
                final Map<Field, List<?>> fieldsToTermsSubsets = Maps.newHashMap();
                fieldsToTermsSubsets.put(field, termSubset);
                return session.ftgsGetSubsetIterator(fieldsToTermsSubsets, callback);
            }
        } else {
            final GroupingFTGSCallback callback = new GroupingFTGSCallback(session.getStackDepth(), statRefs, groupKeys, limits);
            if(!isTermSubset()) {
                session.ftgsIterate(Arrays.asList(field), callback);
            } else {
                final Map<Field, List<?>> fieldsToTermsSubsets = Maps.newHashMap();
                fieldsToTermsSubsets.put(field, termSubset);
                session.ftgsSubsetIterate(fieldsToTermsSubsets, callback);
            }
            return callback.getResults().iterator();
        }
    }

    public Field getField() {
        return field;
    }

    public int getTopK() {
        return topK;
    }

    public boolean isNoExplode() {
        return noExplode;
    }

    public boolean isTopK() {
        return topK != 0;
    }

    public boolean isTermSubset() {
        return termSubset.size() != 0;
    }

    public int getRowLimit() {
        return rowLimit;
    }
}
