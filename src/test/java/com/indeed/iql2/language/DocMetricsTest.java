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

import com.indeed.common.datastruct.PersistentStack;
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
import java.util.function.Function;

import static com.indeed.iql2.language.DocMetric.Add;
import static com.indeed.iql2.language.DocMetric.Divide;
import static com.indeed.iql2.language.DocMetric.Field;
import static com.indeed.iql2.language.DocMetric.Multiply;
import static com.indeed.iql2.language.DocMetric.Subtract;

public class DocMetricsTest {
    public static final WallClock CLOCK = new StoppedClock(new DateTime(2015, 2, 1, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis());
    private static final FieldResolver FIELD_RESOLVER = FieldResolverTest.fromQuery("from synthetic 2d 1d");
    public static final Query.Context CONTEXT = new Query.Context(
            Collections.emptyList(),
            AllData.DATASET.getDatasetsMetadata(),
            null,
            s -> System.out.println("PARSE WARNING: " + s),
            CLOCK,
            new TracingTreeTimer(),
            FIELD_RESOLVER.universalScope(),
            new NullShardResolver(),
            PersistentStack.empty()
    );

    private static final Function<String, DocMetric> PARSE_LEGACY_DOC_METRIC = new Function<String, DocMetric>() {
        public DocMetric apply(@Nullable String input) {
            final JQLParser.DocMetricContext ctx = Queries.runParser(input, new Function<JQLParser, JQLParser.DocMetricContext>() {
                public JQLParser.DocMetricContext apply(JQLParser input) {
                    return input.docMetric(true);
                }
            });
            return DocMetrics.parseDocMetric(ctx, CONTEXT);
        }
    };

    private static final Function<String, DocMetric> PARSE_IQL2_DOC_METRIC = new Function<String, DocMetric>() {
        public DocMetric apply(@Nullable String input) {
            final JQLParser.DocMetricContext ctx = Queries.runParser(input, new Function<JQLParser, JQLParser.DocMetricContext>() {
                public JQLParser.DocMetricContext apply(JQLParser input) {
                    return input.docMetric(false);
                }
            });
            return DocMetrics.parseDocMetric(ctx, CONTEXT);
        }
    };

    private static final Function<String, String> REPLACE_DIVIDES = s -> s.replace("/", "\\");

    public static DocMetric docField(String field) {
        return new Field(FieldSet.of("synthetic", field, true));
    }

    @Test
    public void testAdditivePrecedence() throws Exception {
        for (final boolean useLegacy : new boolean[]{false, true}) {
            CommonArithmetic.testAdditivePrecedence(
                    useLegacy ? PARSE_LEGACY_DOC_METRIC : PARSE_IQL2_DOC_METRIC,
                    Add.create(docField("X"), docField("Y")),
                    new Subtract(docField("X"), docField("Y")),
                    new Subtract(Add.create(docField("X"), docField("Y")), docField("Z")),
                    Add.create(new Subtract(docField("X"), docField("Y")), docField("Z"))
            );
        }
    }

    @Test
    public void testLotsOfArithmetic() throws Exception {
        final DocMetric aTimesBPlusCTimesD = Add.create(new Multiply(docField("A"), docField("B")), new Multiply(docField("C"), docField("D")));
        // "A * B / C * D + (A * B - C * D + E)"
        final DocMetric complex =
            Add.create(
                    new Multiply(new Divide(new Multiply(docField("A"), docField("B")), docField("C")), docField("D")),
                    Add.create(new Subtract(new Multiply(docField("A"), docField("B")), new Multiply(docField("C"), docField("D"))), docField("E"))
            );
        CommonArithmetic.testLotsOfArithmetic(
                PARSE_IQL2_DOC_METRIC,
                aTimesBPlusCTimesD,
                complex
        );

        CommonArithmetic.testLotsOfArithmetic(
                REPLACE_DIVIDES.andThen(PARSE_LEGACY_DOC_METRIC),
                aTimesBPlusCTimesD,
                complex
        );
    }

    @Test
    public void testLog() throws Exception {
        final DocMetric logCount1 = new DocMetric.Log(new DocMetric.Count(), 1);
        Assert.assertEquals(logCount1, PARSE_LEGACY_DOC_METRIC.apply("log(count())"));
        Assert.assertEquals(logCount1, PARSE_IQL2_DOC_METRIC.apply("log(count())"));

        final DocMetric logCount100 = new DocMetric.Log(new DocMetric.Count(), 100);
        Assert.assertEquals(logCount100, PARSE_LEGACY_DOC_METRIC.apply("log(count(), 100)"));
        Assert.assertEquals(logCount100, PARSE_IQL2_DOC_METRIC.apply("log(count(), 100)"));

        final DocMetric logLogCount = new DocMetric.Log(new DocMetric.Log(new DocMetric.Count(), 1), 1);
        Assert.assertEquals(logLogCount, PARSE_LEGACY_DOC_METRIC.apply("log(log(count()))"));
        Assert.assertEquals(logLogCount, PARSE_IQL2_DOC_METRIC.apply("log(log(count()))"));
    }

    @Test
    public void testExp() throws Exception {
        final DocMetric expCount1 = new DocMetric.Exponentiate(new DocMetric.Count(), 1);
        Assert.assertEquals(expCount1, PARSE_LEGACY_DOC_METRIC.apply("exp(count())"));
        Assert.assertEquals(expCount1, PARSE_IQL2_DOC_METRIC.apply("exp(count())"));

        final DocMetric expCount100 = new DocMetric.Exponentiate(new DocMetric.Count(), 100);
        Assert.assertEquals(expCount100, PARSE_LEGACY_DOC_METRIC.apply("exp(count(), 100)"));
        Assert.assertEquals(expCount100, PARSE_IQL2_DOC_METRIC.apply("exp(count(), 100)"));

        final DocMetric expExpCount = new DocMetric.Exponentiate(new DocMetric.Exponentiate(new DocMetric.Count(), 1), 1);
        Assert.assertEquals(expExpCount, PARSE_LEGACY_DOC_METRIC.apply("exp(exp(count()))"));
        Assert.assertEquals(expExpCount, PARSE_IQL2_DOC_METRIC.apply("exp(exp(count()))"));
    }

    @Test
    public void testIfThenElsePrecedence() {
        Assert.assertEquals(
                new DocMetric.IfThenElse(
                        DocFilter.FieldIs.create(FieldSet.of("synthetic", "X", true), Term.term(0)),
                        docField("Y"),
                        new DocMetric.Divide(docField("Z"), new DocMetric.Constant(100))
                ),
                PARSE_IQL2_DOC_METRIC.apply("if X=0 then Y else Z / 100")
        );
        Assert.assertEquals(
                new DocMetric.IfThenElse(
                        DocFilter.FieldIs.create(FieldSet.of("synthetic", "X", true), Term.term(0)),
                        docField("Y"),
                        new DocMetric.Divide(docField("Z"), new DocMetric.Constant(100))
                ),
                PARSE_IQL2_DOC_METRIC.apply("if X=0 then Y else (Z / 100)")
        );
        Assert.assertEquals(
                new DocMetric.Divide(
                    new DocMetric.IfThenElse(
                            DocFilter.FieldIs.create(FieldSet.of("synthetic", "X", true), Term.term(0)),
                            docField("Y"),
                            docField("Z")
                    ),
                    new DocMetric.Constant(100)
                ),
                PARSE_IQL2_DOC_METRIC.apply("(if X=0 then Y else Z) / 100")
        );
    }
}