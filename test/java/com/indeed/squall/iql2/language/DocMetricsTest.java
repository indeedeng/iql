package com.indeed.squall.iql2.language;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.indeed.squall.iql2.language.query.Queries;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.indeed.squall.iql2.language.DocMetric.Add;
import static com.indeed.squall.iql2.language.DocMetric.Divide;
import static com.indeed.squall.iql2.language.DocMetric.Field;
import static com.indeed.squall.iql2.language.DocMetric.Multiply;
import static com.indeed.squall.iql2.language.DocMetric.Subtract;

public class DocMetricsTest {
    private static final Function<String, DocMetric> PARSE_LEGACY_DOC_METRIC = new Function<String, DocMetric>() {
        public DocMetric apply(@Nullable String input) {
            final Map<String, Set<String>> datasetToKeywordAnalyzerFields = Collections.emptyMap();
            final Map<String, Set<String>> datasetToIntFields = Collections.emptyMap();
            final JQLParser.DocMetricContext ctx = Queries.runParser(input, new Function<JQLParser, JQLParser.DocMetricContext>() {
                public JQLParser.DocMetricContext apply(JQLParser input) {
                    return input.docMetric(true);
                }
            });
            return DocMetrics.parseDocMetric(ctx, datasetToKeywordAnalyzerFields, datasetToIntFields);
        }
    };

    private static final Function<String, DocMetric> PARSE_IQL2_DOC_METRIC = new Function<String, DocMetric>() {
        public DocMetric apply(@Nullable String input) {
            final Map<String, Set<String>> datasetToKeywordAnalyzerFields = Collections.emptyMap();
            final Map<String, Set<String>> datasetToIntFields = Collections.emptyMap();
            final JQLParser.DocMetricContext ctx = Queries.runParser(input, new Function<JQLParser, JQLParser.DocMetricContext>() {
                public JQLParser.DocMetricContext apply(JQLParser input) {
                    return input.docMetric(false);
                }
            });
            return DocMetrics.parseDocMetric(ctx, datasetToKeywordAnalyzerFields, datasetToIntFields);
        }
    };

    private static final Function<String, String> REPLACE_DIVIDES = new Function<String, String>() {
        public String apply(String input) {
            return input.replace("/", "\\");
        }
    };


    @Test
    public void testAdditivePrecedence() throws Exception {
        for (final boolean useLegacy : new boolean[]{false, true}) {
            CommonArithmetic.testAdditivePrecedence(
                    useLegacy ? PARSE_LEGACY_DOC_METRIC : PARSE_IQL2_DOC_METRIC,
                    new Add(new Field("X"), new Field("Y")),
                    new Subtract(new Field("X"), new Field("Y")),
                    new Subtract(new Add(new Field("X"), new Field("Y")), new Field("Z")),
                    new Add(new Subtract(new Field("X"), new Field("Y")), new Field("Z"))
            );
        }
    }

    @Test
    public void testLotsOfArithmetic() throws Exception {
        final DocMetric aTimesBPlusCTimesD = new Add(new Multiply(new Field("A"), new Field("B")), new Multiply(new Field("C"), new Field("D")));
        // "A * B / C * D + (A * B - C * D + E)"
        final DocMetric complex =
            new Add(
                    new Multiply(new Divide(new Multiply(new Field("A"), new Field("B")), new Field("C")), new Field("D")),
                    new Add(new Subtract(new Multiply(new Field("A"), new Field("B")), new Multiply(new Field("C"), new Field("D"))), new Field("E"))
            );
        CommonArithmetic.testLotsOfArithmetic(
                PARSE_IQL2_DOC_METRIC,
                aTimesBPlusCTimesD,
                complex
        );

        CommonArithmetic.testLotsOfArithmetic(
                Functions.compose(PARSE_LEGACY_DOC_METRIC, REPLACE_DIVIDES),
                aTimesBPlusCTimesD,
                complex
        );
    }
}