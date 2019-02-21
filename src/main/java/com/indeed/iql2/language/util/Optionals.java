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

package com.indeed.iql2.language.util;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.query.GroupBy;

public class Optionals {
    private Optionals() {
    }

    public static Optional<AggregateFilter> transform(Optional<AggregateFilter> filter, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
        if (filter.isPresent()) {
            return Optional.of(filter.get().transform(f, g, h, i, groupByFunction));
        } else {
            return filter;
        }
    }

    public static Optional<AggregateFilter> traverse1(Optional<AggregateFilter> filter, Function<AggregateMetric, AggregateMetric> f) {
        if (filter.isPresent()) {
            return Optional.of(filter.get().traverse1(f));
        } else {
            return filter;
        }
    }
}
