package com.indeed.squall.iql2.language;

import com.google.common.base.Function;
import com.indeed.common.util.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import com.indeed.squall.iql2.language.DocMetric.Field;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.metadata.DatasetsMetadata;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import javax.annotation.Nullable;

import static com.indeed.squall.iql2.language.AggregateMetric.Add;
import static com.indeed.squall.iql2.language.AggregateMetric.Divide;
import static com.indeed.squall.iql2.language.AggregateMetric.DocStats;
import static com.indeed.squall.iql2.language.AggregateMetric.ImplicitDocStats;
import static com.indeed.squall.iql2.language.AggregateMetric.Multiply;
import static com.indeed.squall.iql2.language.AggregateMetric.Subtract;

public class AggregateMetricsTest {
    private static final WallClock CLOCK = new StoppedClock(new DateTime(2015, 2, 1, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis());

    private static final Function<String, AggregateMetric> PARSE_IQL2_AGGREGATE_METRIC = new Function<String, AggregateMetric>() {
        public AggregateMetric apply(@Nullable String input) {
            final JQLParser.AggregateMetricContext ctx = Queries.runParser(input, new Function<JQLParser, JQLParser.AggregateMetricContext>() {
                public JQLParser.AggregateMetricContext apply(JQLParser input) {
                    return input.aggregateMetric(false);
                }
            });
            return AggregateMetrics.parseAggregateMetric(ctx, DatasetsMetadata.empty(), new Consumer<String>() {
                @Override
                public void accept(String s) {
                    System.out.println("PARSE WARNING: " + s);
                }
            }, CLOCK);
        }
    };
    public static final Function<String, AggregateMetric> PARSE_LEGACY_AGGREGATE_METRIC = new Function<String, AggregateMetric>() {
        public AggregateMetric apply(@Nullable String input) {
            final JQLParser.AggregateMetricContext ctx = Queries.runParser(input, new Function<JQLParser, JQLParser.AggregateMetricContext>() {
                public JQLParser.AggregateMetricContext apply(JQLParser input) {
                    return input.aggregateMetric(true);
                }
            });
            return AggregateMetrics.parseAggregateMetric(ctx, DatasetsMetadata.empty(), new Consumer<String>() {
                @Override
                public void accept(String s) {
                    System.out.println("PARSE WARNING: " + s);
                }
            }, CLOCK);
        }
    };

    private static AggregateMetric field(String field) {
        return new ImplicitDocStats(new Field(field));
    }

    @Test
    public void testIQL2AdditivePrecedence() throws Exception {
        CommonArithmetic.testAdditivePrecedence(
                PARSE_IQL2_AGGREGATE_METRIC,
                new Add(field("X"), field("Y")),
                new Subtract(field("X"), field("Y")),
                new Subtract(new Add(field("X"), field("Y")), field("Z")),
                new Add(new Subtract(field("X"), field("Y")), field("Z"))
        );
    }

    @Test
    public void testIQL2LotsOfArithmetic() throws Exception {
        CommonArithmetic.testLotsOfArithmetic(
                PARSE_IQL2_AGGREGATE_METRIC,
                new Add(new Multiply(field("A"), field("B")), new Multiply(field("C"), field("D"))),
                // "A * B / C * D + (A * B - C * D + E)"
                new Add(
                        new Multiply(new Divide(new Multiply(field("A"), field("B")), field("C")), field("D")),
                        new Add(new Subtract(new Multiply(field("A"), field("B")), new Multiply(field("C"), field("D"))), field("E"))
                )
        );
    }

    @Test
    public void testV1AdditivePrecedence() throws Exception {
        CommonArithmetic.testAdditivePrecedence(
                PARSE_LEGACY_AGGREGATE_METRIC,
                new ImplicitDocStats(new DocMetric.Add(new Field("X"), new Field("Y"))),
                new ImplicitDocStats(new DocMetric.Subtract(new Field("X"), new Field("Y"))),
                new ImplicitDocStats(new DocMetric.Subtract(new DocMetric.Add(new Field("X"), new Field("Y")), new Field("Z"))),
                new ImplicitDocStats(new DocMetric.Add(new DocMetric.Subtract(new Field("X"), new Field("Y")), new Field("Z")))
        );
    }


    @Test
    public void testV1LotsOfArithmetic() throws Exception {
        CommonArithmetic.testLotsOfArithmetic(
                PARSE_LEGACY_AGGREGATE_METRIC,
                new ImplicitDocStats(new DocMetric.Add(new DocMetric.Multiply(new Field("A"), new Field("B")), new DocMetric.Multiply(new Field("C"), new Field("D")))),
                // "A * B / C * D + (A * B - C * D + E)"
                new Divide(
                        new DocStats(new DocMetric.Multiply(new Field("A"), new Field("B"))),
                        new DocStats(
                                new DocMetric.Add(
                                        new DocMetric.Multiply(new Field("C"), new Field("D")),
                                        new DocMetric.Add(
                                                new DocMetric.Subtract(
                                                        new DocMetric.Multiply(new Field("A"), new Field("B")),
                                                        new DocMetric.Multiply(new Field("C"), new Field("D"))
                                                ),
                                                new Field("E")
                                        )
                                )
                        )
                )
        );
    }
}
