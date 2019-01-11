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
import com.google.common.collect.ImmutableList;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldResolver;
import com.indeed.iql2.language.query.fieldresolution.FieldResolverTest;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Collections;

import static com.indeed.iql2.language.AggregateMetric.Divide;
import static com.indeed.iql2.language.AggregateMetric.DocStats;
import static com.indeed.iql2.language.AggregateMetric.Multiply;
import static com.indeed.iql2.language.AggregateMetric.Subtract;
import static com.indeed.iql2.language.DocMetricsTest.docField;

public class AggregateMetricsTest {
    private static final WallClock CLOCK = new StoppedClock(new DateTime(2015, 2, 1, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis());
    public static final FieldResolver FIELD_RESOLVER = FieldResolverTest.fromQuery("from synthetic 2d 1d");
    private static final Query.Context CONTEXT = new Query.Context(
            Collections.emptyList(),
            AllData.DATASET.getDatasetsMetadata(),
            null,
            s -> System.out.println("PARSE WARNING: " + s),
            CLOCK,
            FIELD_RESOLVER.universalScope()
    );

    private static final Function<String, AggregateMetric> PARSE_IQL2_AGGREGATE_METRIC = new Function<String, AggregateMetric>() {
        public AggregateMetric apply(@Nullable String input) {
            final JQLParser.AggregateMetricContext ctx = Queries.runParser(input, new Function<JQLParser, JQLParser.AggregateMetricContext>() {
                public JQLParser.AggregateMetricContext apply(JQLParser input) {
                    return input.aggregateMetric(false);
                }
            });
            return AggregateMetrics.parseAggregateMetric(ctx, CONTEXT);
        }
    };
    public static final Function<String, AggregateMetric> PARSE_LEGACY_AGGREGATE_METRIC = new Function<String, AggregateMetric>() {
        public AggregateMetric apply(@Nullable String input) {
            final JQLParser.AggregateMetricContext ctx = Queries.runParser(input, new Function<JQLParser, JQLParser.AggregateMetricContext>() {
                public JQLParser.AggregateMetricContext apply(JQLParser input) {
                    return input.aggregateMetric(true);
                }
            });
            return AggregateMetrics.parseAggregateMetric(ctx, CONTEXT);
        }
    };


    private static AggregateMetric aggField(String field) {
        return new AggregateMetric.DocStats(new DocMetric.Field(FieldSet.of("synthetic", field)));
    }

    private static DocMetric add(final DocMetric m1, final DocMetric m2) {
        return DocMetric.Add.create(ImmutableList.of(m1, m2));
    }

    private static AggregateMetric add(final AggregateMetric m1, final AggregateMetric m2) {
        return AggregateMetric.Add.create(ImmutableList.of(m1, m2));
    }

    @Test
    public void testIQL2AdditivePrecedence() throws Exception {
        CommonArithmetic.testAdditivePrecedence(
                PARSE_IQL2_AGGREGATE_METRIC,
                add(aggField("X"), aggField("Y")),
                new Subtract(aggField("X"), aggField("Y")),
                new Subtract(add(aggField("X"), aggField("Y")), aggField("Z")),
                add(new Subtract(aggField("X"), aggField("Y")), aggField("Z"))
        );
    }

    @Test
    public void testIQL2LotsOfArithmetic() throws Exception {
        CommonArithmetic.testLotsOfArithmetic(
                PARSE_IQL2_AGGREGATE_METRIC,
                add(new Multiply(aggField("A"), aggField("B")), new Multiply(aggField("C"), aggField("D"))),
                // "A * B / C * D + (A * B - C * D + E)"
                add(
                        new Multiply(new Divide(new Multiply(aggField("A"), aggField("B")), aggField("C")), aggField("D")),
                        add(new Subtract(new Multiply(aggField("A"), aggField("B")), new Multiply(aggField("C"), aggField("D"))), aggField("E"))
                )
        );
    }

    @Test
    public void testV1AdditivePrecedence() throws Exception {
        CommonArithmetic.testAdditivePrecedence(
                PARSE_LEGACY_AGGREGATE_METRIC,
                new AggregateMetric.DocStats(add(docField("X"), docField("Y"))),
                new AggregateMetric.DocStats(new DocMetric.Subtract(docField("X"), docField("Y"))),
                new AggregateMetric.DocStats(new DocMetric.Subtract(add(docField("X"), docField("Y")), docField("Z"))),
                new DocStats(add(new DocMetric.Subtract(docField("X"), docField("Y")), docField("Z")))
        );
    }


    @Test
    public void testV1LotsOfArithmetic() throws Exception {
        CommonArithmetic.testLotsOfArithmetic(
                PARSE_LEGACY_AGGREGATE_METRIC,
                new AggregateMetric.DocStats(add(new DocMetric.Multiply(docField("A"), docField("B")), new DocMetric.Multiply(docField("C"), docField("D")))),
                // "A * B / C * D + (A * B - C * D + E)"
                new Divide(
                        new AggregateMetric.DocStats(new DocMetric.Multiply(docField("A"), docField("B"))),
                        new AggregateMetric.DocStats(
                                add(
                                        new DocMetric.Multiply(docField("C"), docField("D")),
                                        add(
                                                new DocMetric.Subtract(
                                                        new DocMetric.Multiply(docField("A"), docField("B")),
                                                        new DocMetric.Multiply(docField("C"), docField("D"))
                                                ),
                                                docField("E")
                                        )
                                )
                        )
                )
        );
    }
}
