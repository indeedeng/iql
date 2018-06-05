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

package com.indeed.squall.iql2.execution.commands.misc;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.execution.AggregateFilter;
import com.indeed.squall.iql2.execution.AggregateFilters;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetrics;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;

import java.util.Arrays;

public class FieldIterateOpts {
    public Optional<Integer> limit = Optional.absent();
    public Optional<TopK> topK = Optional.absent();
    public Optional<AggregateFilter> filter = Optional.absent();
    public Optional<long[]> sortedIntTermSubset = Optional.absent();
    public Optional<String[]> sortedStringTermSubset = Optional.absent();

    public FieldIterateOpts copy() {
        final FieldIterateOpts result = new FieldIterateOpts();
        result.limit = this.limit;
        result.topK = this.topK;
        result.filter = this.filter;
        return result;
    }
}
