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

package com.indeed.iql2.language;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.iql2.language.query.GroupBy;
import lombok.Data;

// TODO: Make Positional
@Data
public class GroupByEntry {
    public final GroupBy groupBy;
    public final Optional<AggregateFilter> filter;
    public final Optional<String> alias;

    public GroupByEntry transform(
            final Function<GroupBy, GroupBy> groupByF,
            final Function<AggregateMetric, AggregateMetric> f,
            final Function<DocMetric, DocMetric> g,
            final Function<AggregateFilter, AggregateFilter> h,
            final Function<DocFilter, DocFilter> i) {
        return new GroupByEntry(groupBy.transform(groupByF, f, g, h, i), filter.transform(new Function<AggregateFilter, AggregateFilter>() {
            public AggregateFilter apply(AggregateFilter input) {
                return input.transform(f, g, h, i, groupByF);
            }
        }), alias);
    }

    public GroupByEntry traverse1(final Function<AggregateMetric, AggregateMetric> f) {
        return new GroupByEntry(groupBy.traverse1(f), filter.transform(new Function<AggregateFilter, AggregateFilter>() {
            public AggregateFilter apply(AggregateFilter input) {
                return input.traverse1(f);
            }
        }), alias);
    }
}
