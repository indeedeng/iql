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

package com.indeed.squall.iql2.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetrics;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;

public class AggregateFilters {
    public static AggregateFilter fromJson(final JsonNode node, final Function<String, PerGroupConstant> namedMetricLookup, final GroupKeySet groupKeySet) {
        final Supplier<AggregateMetric> m1 = new Supplier<AggregateMetric>() {
            @Override
            public AggregateMetric get() {
                return AggregateMetrics.fromJson(node.get("arg1"), namedMetricLookup, groupKeySet);
            }
        };
        final Supplier<AggregateMetric> m2 = new Supplier<AggregateMetric>() {
            @Override
            public AggregateMetric get() {
                return AggregateMetrics.fromJson(node.get("arg2"), namedMetricLookup, groupKeySet);
            }
        };
        final Supplier<AggregateFilter> f1 = new Supplier<AggregateFilter>() {
            public AggregateFilter get() {
                return fromJson(node.get("arg1"), namedMetricLookup, groupKeySet);
            }
        };
        final Supplier<AggregateFilter> f2 = new Supplier<AggregateFilter>() {
            public AggregateFilter get() {
                return fromJson(node.get("arg2"), namedMetricLookup, groupKeySet);
            }
        };
        switch (node.get("type").textValue()) {
            case "termEquals":
                return new AggregateFilter.TermEquals(Term.fromJson(node.get("value")));
            case "termEqualsRegex":
                return new AggregateFilter.TermEqualsRegex(Term.fromJson(node.get("value")));
            case "not":
                return new AggregateFilter.Not(fromJson(node.get("value"), namedMetricLookup, groupKeySet));
            case "regex":
                return new AggregateFilter.RegexFilter(node.get("value").textValue());
            case "metricEquals":
                return new AggregateFilter.MetricEquals(m1.get(), m2.get());
            case "metricNotEquals":
                return new AggregateFilter.MetricNotEquals(m1.get(), m2.get());
            case "greaterThan":
                return new AggregateFilter.GreaterThan(m1.get(), m2.get());
            case "greaterThanOrEquals":
                return new AggregateFilter.GreaterThanOrEquals(m1.get(), m2.get());
            case "lessThan":
                return new AggregateFilter.LessThan(m1.get(), m2.get());
            case "lessThanOrEquals":
                return new AggregateFilter.LessThanOrEquals(m1.get(), m2.get());
            case "and":
                return new AggregateFilter.And(f1.get(), f2.get());
            case "or":
                return new AggregateFilter.Or(f1.get(), f2.get());
            case "always":
                return new AggregateFilter.Constant(true);
            case "never":
                return new AggregateFilter.Constant(false);
            case "isDefaultGroup":
                return new AggregateFilter.IsDefaultGroup(groupKeySet);
        }
        throw new RuntimeException("Oops: " + node);
    }
}
