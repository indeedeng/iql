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

package com.indeed.squall.iql2.language;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.query.GroupBy;

import java.util.Objects;

// TODO: Make Positional
public class GroupByMaybeHaving {
    public final GroupBy groupBy;
    public final Optional<AggregateFilter> filter;
    public final Optional<String> alias;

    public GroupByMaybeHaving(GroupBy groupBy, Optional<AggregateFilter> filter, Optional<String> alias) {
        this.groupBy = groupBy;
        this.filter = filter;
        this.alias = alias;
    }

    public GroupByMaybeHaving transform(
            final Function<GroupBy, GroupBy> groupByF,
            final Function<AggregateMetric, AggregateMetric> f,
            final Function<DocMetric, DocMetric> g,
            final Function<AggregateFilter, AggregateFilter> h,
            final Function<DocFilter, DocFilter> i) {
        return new GroupByMaybeHaving(groupBy.transform(groupByF, f, g, h, i), filter.transform(new Function<AggregateFilter, AggregateFilter>() {
            public AggregateFilter apply(AggregateFilter input) {
                return input.transform(f, g, h, i, groupByF);
            }
        }), alias);
    }

    public GroupByMaybeHaving traverse1(final Function<AggregateMetric, AggregateMetric> f) {
        return new GroupByMaybeHaving(groupBy.traverse1(f), filter.transform(new Function<AggregateFilter, AggregateFilter>() {
            public AggregateFilter apply(AggregateFilter input) {
                return input.traverse1(f);
            }
        }), alias);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupByMaybeHaving that = (GroupByMaybeHaving) o;
        return Objects.equals(groupBy, that.groupBy) &&
                Objects.equals(filter, that.filter) &&
                Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupBy, filter, alias);
    }

    @Override
    public String toString() {
        return "GroupByMaybeHaving{" +
                "groupBy=" + groupBy +
                ", filter=" + filter +
                ", alias=" + alias +
                '}';
    }
}
