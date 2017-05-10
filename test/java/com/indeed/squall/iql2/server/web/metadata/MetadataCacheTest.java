package com.indeed.squall.iql2.server.web.metadata;

import com.google.common.collect.ImmutableMap;
import com.indeed.ims.client.yamlFile.MetricsYaml;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocMetric;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
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
        Assert.assertEquals("c1+c2*c3", MetadataCache.expandMetricExpression("complex", metricsExpressions));
        Assert.assertEquals("avg(c1+c2*c3)", MetadataCache.expandMetricExpression("avg(complex)", metricsExpressions));
        Assert.assertEquals("(a1+same)", MetadataCache.expandMetricExpression("(a1+same)", metricsExpressions));
        Assert.assertEquals("a2+c1+c2*c3", MetadataCache.expandMetricExpression("a2+alias", metricsExpressions));
        Assert.assertEquals("(c1+c2*c3*10)/c1+c2*c3+same", MetadataCache.expandMetricExpression("(complex*10)/recursive", metricsExpressions));
    }

    @Test
    public void testParseMetric() {
        final MetadataCache metadataCache = new MetadataCache(null, null);

        final Map<String, String> metricsExpressions = new HashMap<>();
        final MetricsYaml countsMetric = new MetricsYaml();
        countsMetric.setName("counts");
        countsMetric.setExpr("count()");
        metricsExpressions.put(countsMetric.getName(), countsMetric.getExpr());
        Assert.assertEquals(new AggregateMetric.ImplicitDocStats(new DocMetric.Count()), metadataCache.parseMetric(countsMetric, metricsExpressions));

        final MetricsYaml sameMetric = new MetricsYaml();
        sameMetric.setName("same");
        sameMetric.setExpr("same");
        metricsExpressions.put(sameMetric.getName(), sameMetric.getExpr());
        Assert.assertEquals(new AggregateMetric.ImplicitDocStats(new DocMetric.Field("SAME")), metadataCache.parseMetric(sameMetric, metricsExpressions));

        final MetricsYaml calcMetric = new MetricsYaml();
        calcMetric.setName("complex");
        calcMetric.setExpr("(a1+a2)*10");
        metricsExpressions.put(calcMetric.getName(), calcMetric.getExpr());
        Assert.assertEquals(
                new AggregateMetric.ImplicitDocStats(
                        new DocMetric.Multiply(new DocMetric.Add(new DocMetric.Field("A1"), new DocMetric.Field("A2")),new DocMetric.Constant(10))),
                metadataCache.parseMetric(calcMetric, metricsExpressions));

        final MetricsYaml combinedMetric = new MetricsYaml();
        combinedMetric.setName("combined");
        combinedMetric.setExpr("same+complex");
        metricsExpressions.put(combinedMetric.getName(), combinedMetric.getExpr());
        Assert.assertEquals(
                new AggregateMetric.ImplicitDocStats(
                        new DocMetric.Add(
                                new DocMetric.Field("SAME"),
                                new DocMetric.Multiply(new DocMetric.Add(new DocMetric.Field("A1"), new DocMetric.Field("A2")), new DocMetric.Constant(10)))),
                metadataCache.parseMetric(combinedMetric, metricsExpressions));


        final MetricsYaml aggregateMetric1 = new MetricsYaml();
        aggregateMetric1.setName("agg1");
        aggregateMetric1.setExpr("oji/ojc");
        metricsExpressions.put(aggregateMetric1.getName(), aggregateMetric1.getExpr());
        Assert.assertEquals(
                new AggregateMetric.Divide(
                        new AggregateMetric.DocStats(new DocMetric.Field("OJI")),
                        new AggregateMetric.DocStats(new DocMetric.Field("OJC"))),
                metadataCache.parseMetric(aggregateMetric1, metricsExpressions));


        final MetricsYaml aggregateMetric2 = new MetricsYaml();
        aggregateMetric2.setName("agg2");
        aggregateMetric2.setExpr("(score-100)/4");
        metricsExpressions.put(aggregateMetric2.getName(), aggregateMetric2.getExpr());
        Assert.assertEquals(
                new AggregateMetric.Divide(
                        new AggregateMetric.DocStats(new DocMetric.Subtract(new DocMetric.Field("SCORE"), new DocMetric.Constant(100))),
                        new AggregateMetric.Constant(4)),
                metadataCache.parseMetric(aggregateMetric2, metricsExpressions));


        final MetricsYaml requireFTGSMetric = new MetricsYaml();
        requireFTGSMetric.setName("ftgsFunc");
        requireFTGSMetric.setExpr("PERCENTILE(oji, 95)");
        metricsExpressions.put(requireFTGSMetric.getName(), requireFTGSMetric.getExpr());
        try {
            metadataCache.parseMetric(requireFTGSMetric, metricsExpressions);
            Assert.fail("require FTGS func is not supported");
        } catch (UnsupportedOperationException ex) {
        }
    }
}