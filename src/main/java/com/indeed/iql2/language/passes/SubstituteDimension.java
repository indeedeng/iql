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

package com.indeed.iql2.language.passes;

import com.google.common.base.Preconditions;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql.metadata.MetricMetadata;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;
import com.indeed.iql2.language.query.shardresolution.NullShardResolver;
import org.apache.log4j.Logger;

public class SubstituteDimension {
    private static final Logger log = Logger.getLogger(SubstituteDimension.class);

    private SubstituteDimension() {
    }

    private static AggregateMetric parseDimensionsMetric(final String name, final String expr, final DatasetsMetadata datasetsMetadata, final ScopedFieldResolver fieldResolver, final Query.Context context) {
        Preconditions.checkNotNull(expr);

        final AggregateMetric dimensionMetric = Queries.parseAggregateMetric(
                expr,
                true,
                new Query.Context(
                        context.options,
                        datasetsMetadata,
                        context.fromContext,
                        s -> log.warn(String.format("parse DimensionMetric name: %s, expr: %s, warning: %s", name, expr, s)),
                        context.clock,
                        context.timer,
                        fieldResolver,
                        new NullShardResolver(), // Dimensions metrics cannot use subqueries
                        context.aggregateContexts,
                        context.timeZone)
        );

        Preconditions.checkArgument(!dimensionMetric.requiresFTGS(), "Dimension metric requiring FTGS is not supported");

        return dimensionMetric;
    }

    public static AggregateMetric getAggregateMetric(final MetricMetadata metricMetadata, final DatasetsMetadata datasetsMetadata, final ScopedFieldResolver scopedFieldResolver, final Query.Context context) {
        return parseDimensionsMetric(metricMetadata.getName(), metricMetadata.getExpression(), datasetsMetadata, scopedFieldResolver, context);
    }

    public static DocMetric getDocMetricOrThrow(final MetricMetadata metricMetadata, final DatasetsMetadata datasetsMetadata, final ScopedFieldResolver scopedFieldResolver, final Query.Context context) {
        final AggregateMetric metric = parseDimensionsMetric(metricMetadata.getName(), metricMetadata.getExpression(), datasetsMetadata, scopedFieldResolver, context);
        if (!(metric instanceof AggregateMetric.DocStats)) {
            throw new IllegalArgumentException(
                    String.format("Cannot use compound metrics in per-document context, metric [ %s: %s ]",
                            metricMetadata.getName(), metricMetadata.getExpression()));
        } else {
            return ((AggregateMetric.DocStats) metric).docMetric;
        }
    }
}
