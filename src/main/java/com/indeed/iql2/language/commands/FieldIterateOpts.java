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

package com.indeed.iql2.language.commands;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.primitives.Longs;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.AggregateFilter;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Arrays;
import java.util.Set;

@EqualsAndHashCode
@ToString
public class FieldIterateOpts {
    public Optional<Integer> limit = Optional.absent();
    public Optional<TopK> topK = Optional.absent();
    public Optional<AggregateFilter> filter = Optional.absent();
    public Optional<Set<Long>> intTermSubset = Optional.absent();
    public Optional<Set<String>> stringTermSubset = Optional.absent();

    public com.indeed.iql2.execution.commands.misc.FieldIterateOpts toExecution(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
        final com.indeed.iql2.execution.commands.misc.FieldIterateOpts result = new com.indeed.iql2.execution.commands.misc.FieldIterateOpts();
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
