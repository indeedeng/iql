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
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldResolver;
import com.indeed.iql2.language.query.fieldresolution.FieldResolverTest;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.query.shardresolution.NullShardResolver;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import com.indeed.util.logging.TracingTreeTimer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Collections;

import static com.indeed.iql2.language.AggregateMetric.Add;
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
            new TracingTreeTimer(),
            FIELD_RESOLVER.universalScope(),
            new NullShardResolver()
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

    @Test
    public void testIQL2AdditivePrecedence() throws Exception {
        CommonArithmetic.testAdditivePrecedence(
                PARSE_IQL2_AGGREGATE_METRIC,
                Add.create(aggField("X"), aggField("Y")),
                new Subtract(aggField("X"), aggField("Y")),
                new Subtract(Add.create(aggField("X"), aggField("Y")), aggField("Z")),
                Add.create(new Subtract(aggField("X"), aggField("Y")), aggField("Z"))
        );
    }

    @Test
    public void testIQL2LotsOfArithmetic() throws Exception {
        CommonArithmetic.testLotsOfArithmetic(
                PARSE_IQL2_AGGREGATE_METRIC,
                Add.create(new Multiply(aggField("A"), aggField("B")), new Multiply(aggField("C"), aggField("D"))),
                // "A * B / C * D + (A * B - C * D + E)"
                Add.create(
                        new Multiply(new Divide(new Multiply(aggField("A"), aggField("B")), aggField("C")), aggField("D")),
                        Add.create(new Subtract(new Multiply(aggField("A"), aggField("B")), new Multiply(aggField("C"), aggField("D"))), aggField("E"))
                )
        );
    }

    @Test
    public void testV1AdditivePrecedence() throws Exception {
        CommonArithmetic.testAdditivePrecedence(
                PARSE_LEGACY_AGGREGATE_METRIC,
                new AggregateMetric.DocStats(DocMetric.Add.create(docField("X"), docField("Y"))),
                new AggregateMetric.DocStats(new DocMetric.Subtract(docField("X"), docField("Y"))),
                new AggregateMetric.DocStats(new DocMetric.Subtract(DocMetric.Add.create(docField("X"), docField("Y")), docField("Z"))),
                new DocStats(DocMetric.Add.create(new DocMetric.Subtract(docField("X"), docField("Y")), docField("Z")))
        );
    }


    @Test
    public void testV1LotsOfArithmetic() throws Exception {
        CommonArithmetic.testLotsOfArithmetic(
                PARSE_LEGACY_AGGREGATE_METRIC,
                new AggregateMetric.DocStats(DocMetric.Add.create(new DocMetric.Multiply(docField("A"), docField("B")), new DocMetric.Multiply(docField("C"), docField("D")))),
                // "A * B / C * D + (A * B - C * D + E)"
                new Divide(
                        new AggregateMetric.DocStats(new DocMetric.Multiply(docField("A"), docField("B"))),
                        new AggregateMetric.DocStats(
                                DocMetric.Add.create(
                                        new DocMetric.Multiply(docField("C"), docField("D")),
                                        DocMetric.Add.create(
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

    @Test
    public void testIfThenElsePrecedence() {
        Assert.assertEquals(
                new AggregateMetric.IfThenElse(
                        new AggregateFilter.MetricIs(aggField("X"), new AggregateMetric.Constant(0)),
                        aggField("Y"),
                        new Divide(aggField("Z"), new AggregateMetric.Constant(100))
                ),
                PARSE_IQL2_AGGREGATE_METRIC.apply("if X=0 then Y else Z / 100")
        );
        Assert.assertEquals(
                new AggregateMetric.IfThenElse(
                        new AggregateFilter.MetricIs(aggField("X"), new AggregateMetric.Constant(0)),
                        aggField("Y"),
                        new Divide(aggField("Z"), new AggregateMetric.Constant(100))
                ),
                PARSE_IQL2_AGGREGATE_METRIC.apply("if X=0 then Y else (Z / 100)")
        );
        Assert.assertEquals(
                new Divide(
                        new AggregateMetric.IfThenElse(
                                new AggregateFilter.MetricIs(aggField("X"), new AggregateMetric.Constant(0)),
                                aggField("Y"),
                                aggField("Z")
                        ),
                        new AggregateMetric.Constant(100)
                ),
                PARSE_IQL2_AGGREGATE_METRIC.apply("(if X=0 then Y else Z) / 100")
        );
    }
}
