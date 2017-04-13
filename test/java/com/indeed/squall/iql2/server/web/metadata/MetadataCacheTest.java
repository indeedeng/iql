package com.indeed.squall.iql2.server.web.metadata;

import com.google.common.collect.ImmutableMap;
import com.indeed.ims.client.yamlFile.MetricsYaml;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.language.query.Queries;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataCacheTest {
    @Test
    public void testExpandMetricExpression() {
        final Map<String, String> metricsExpressions = ImmutableMap.of(
                "same", "same",
                "complex", "c1+c2*c3",
                "alias", "complex",
                "recursive", "alias+same"
        );
        Assert.assertEquals("plain", MetadataCache.expandMetricExpression("plain", metricsExpressions));
        Assert.assertEquals("avg(plain)", MetadataCache.expandMetricExpression("avg(plain)", metricsExpressions));
        Assert.assertEquals("(a1+same)", MetadataCache.expandMetricExpression("(a1+same)", metricsExpressions));
        Assert.assertEquals("a2+c1+c2*c3", MetadataCache.expandMetricExpression("a2+alias", metricsExpressions));
        Assert.assertEquals("(c1+c2*c3*10)/c1+c2*c3+same", MetadataCache.expandMetricExpression("(complex*10)/recursive", metricsExpressions));
    }

    @Test
    public void testParseDataset() throws Exception {
        final MetadataCache metadataCache = new MetadataCache(null, null);
        final List<MetricsYaml> metrics = new ArrayList<>();
        final Map<String, AggregateMetric> metricExpressions = new HashMap<>();
        createMetrics(metrics, metricExpressions);
        final DatasetDimensions parsedDimension = metadataCache.parseMetrics("dumb", metrics.toArray(new MetricsYaml[metrics.size()]));
        for (String metricName : parsedDimension.fields()) {
            Assert.assertEquals(metricExpressions.get(metricName), parsedDimension.getMetric(metricName).get());
        }
    }

    private void createMetrics(List<MetricsYaml> metrics, Map<String, AggregateMetric> metricExpressions) {
        final MetricsYaml countsMetric = new MetricsYaml();
        countsMetric.setName("counts");
        countsMetric.setExpr("count()");
        metrics.add(countsMetric);
        metricExpressions.put(countsMetric.getName().toUpperCase(), new AggregateMetric.ImplicitDocStats(new DocMetric.Count()));

        final MetricsYaml emptyMetric = new MetricsYaml();
        emptyMetric.setName("empty");
        metrics.add(emptyMetric);
        metricExpressions.put(emptyMetric.getName().toUpperCase(), new AggregateMetric.ImplicitDocStats(new DocMetric.Field("EMPTY")));

        final MetricsYaml sameMetric = new MetricsYaml();
        sameMetric.setName("same");
        sameMetric.setExpr("same");
        metrics.add(sameMetric);
        metricExpressions.put(sameMetric.getName().toUpperCase(), new AggregateMetric.ImplicitDocStats(new DocMetric.Field("SAME")));

        final MetricsYaml calcMetric = new MetricsYaml();
        calcMetric.setName("complex");
        calcMetric.setExpr("(a1+a2)*10");
        metrics.add(calcMetric);
        metricExpressions.put(calcMetric.getName().toUpperCase(), new AggregateMetric.ImplicitDocStats(
                new DocMetric.Multiply(new DocMetric.Add(new DocMetric.Field("A1"), new DocMetric.Field("A2")), new DocMetric.Constant(10))));

        final MetricsYaml combinedMetric = new MetricsYaml();
        combinedMetric.setName("combined");
        combinedMetric.setExpr("same+complex");
        metrics.add(combinedMetric);
        metricExpressions.put(combinedMetric.getName().toUpperCase(), new AggregateMetric.ImplicitDocStats(
                new DocMetric.Add(
                        new DocMetric.Field("SAME"),
                        new DocMetric.Multiply(new DocMetric.Add(new DocMetric.Field("A1"), new DocMetric.Field("A2")), new DocMetric.Constant(10)))));


        final MetricsYaml aggregateMetric1 = new MetricsYaml();
        aggregateMetric1.setName("agg1");
        aggregateMetric1.setExpr("oji/ojc");
        metrics.add(aggregateMetric1);
        metricExpressions.put(aggregateMetric1.getName().toUpperCase(), new AggregateMetric.Divide(
                new AggregateMetric.DocStats(new DocMetric.Field("OJI")),
                new AggregateMetric.DocStats(new DocMetric.Field("OJC"))));

        final MetricsYaml aggregateMetric2 = new MetricsYaml();
        aggregateMetric2.setName("agg2");
        aggregateMetric2.setExpr("(score-100)/4");
        metrics.add(aggregateMetric2);
        metricExpressions.put(aggregateMetric2.getName().toUpperCase(), new AggregateMetric.Divide(
                new AggregateMetric.DocStats(new DocMetric.Subtract(new DocMetric.Field("SCORE"), new DocMetric.Constant(100))),
                new AggregateMetric.Constant(4)));

        final MetricsYaml aggregateMetricFunc = new MetricsYaml();
        aggregateMetricFunc.setName("aggfunc");
        aggregateMetricFunc.setExpr("PERCENTILE(oji, 95)");
        metrics.add(aggregateMetricFunc);
        metricExpressions.put(aggregateMetricFunc.getName().toUpperCase(), getMetric(aggregateMetricFunc.getExpr()));
    }

    private static AggregateMetric getMetric(final String expression) {
        return Queries.parseAggregateMetric(expression, true, Collections.emptyMap(), Collections.emptyMap(), new Consumer<String>() {
            @Override
            public void accept(final String s) {
            }
        });
    }
}