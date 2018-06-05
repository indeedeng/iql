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

package com.indeed.squall.iql2.language.commands;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.primitives.Longs;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.language.AggregateFilter;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public class FieldIterateOpts {
    public Optional<Integer> limit = Optional.absent();
    public Optional<TopK> topK = Optional.absent();
    public Optional<AggregateFilter> filter = Optional.absent();
    public Optional<Set<Long>> intTermSubset = Optional.absent();
    public Optional<Set<String>> stringTermSubset = Optional.absent();

    public FieldIterateOpts copy() {
        final FieldIterateOpts result = new FieldIterateOpts();
        result.limit = this.limit;
        result.topK = this.topK;
        result.filter = this.filter;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldIterateOpts that = (FieldIterateOpts) o;
        return Objects.equals(limit, that.limit) &&
                Objects.equals(topK, that.topK) &&
                Objects.equals(filter, that.filter) &&
                Objects.equals(intTermSubset, that.intTermSubset) &&
                Objects.equals(stringTermSubset, that.stringTermSubset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit, topK, filter, intTermSubset, stringTermSubset);
    }

    @Override
    public String toString() {
        return "FieldIterateOpts{" +
                "limit=" + limit +
                ", topK=" + topK +
                ", filter=" + filter +
                ", intTermSubset=" + intTermSubset +
                ", stringTermSubset=" + stringTermSubset +
                '}';
    }

    public com.indeed.squall.iql2.execution.commands.misc.FieldIterateOpts toExecution(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
        final com.indeed.squall.iql2.execution.commands.misc.FieldIterateOpts result = new com.indeed.squall.iql2.execution.commands.misc.FieldIterateOpts();
        result.filter = filter.transform(x -> x.toExecutionFilter(namedMetricLookup, groupKeySet));
        result.limit = limit;
        result.sortedIntTermSubset = intTermSubset.transform(x -> {
            final long[] terms = Longs.toArray(x);
            Arrays.sort(terms);
            return terms;
        });
        result.sortedStringTermSubset = stringTermSubset.transform(x -> {
            final String[] terms = x.toArray(new String[0]);
            Arrays.sort(terms);
            return terms;
        });
        result.topK = topK.transform(x -> x.toExecution(namedMetricLookup, groupKeySet));
        return result;
    }
}
