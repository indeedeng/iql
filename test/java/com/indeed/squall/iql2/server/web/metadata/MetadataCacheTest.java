package com.indeed.squall.iql2.server.web.metadata;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.indeed.ims.client.ImsClient;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.ims.client.yamlFile.DatasetYaml;
import com.indeed.ims.client.yamlFile.FieldsYaml;
import com.indeed.ims.client.yamlFile.MetricsYaml;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.language.dimensions.Dimension;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import org.junit.Assert;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public class MetadataCacheTest {
    @Test
    public void testParseMetric() {
        final MetadataCache metadataCache = new MetadataCache(null, null);

        final MetricsYaml countsMetric = new MetricsYaml();
        countsMetric.setName("counts");
        countsMetric.setExpr("count()");
        Assert.assertEquals(new AggregateMetric.ImplicitDocStats(new DocMetric.Count()), metadataCache.parseMetric(countsMetric));

        final MetricsYaml sameMetric = new MetricsYaml();
        sameMetric.setName("same");
        sameMetric.setExpr("same");
        Assert.assertEquals(new AggregateMetric.ImplicitDocStats(new DocMetric.Field("SAME")), metadataCache.parseMetric(sameMetric));

        final MetricsYaml calcMetric = new MetricsYaml();
        calcMetric.setName("complex");
        calcMetric.setExpr("(a1+a2)*10");
        Assert.assertEquals(
                new AggregateMetric.ImplicitDocStats(
                        new DocMetric.Multiply(new DocMetric.Add(new DocMetric.Field("A1"), new DocMetric.Field("A2")), new DocMetric.Constant(10))),
                metadataCache.parseMetric(calcMetric));

        final MetricsYaml aggregateMetric1 = new MetricsYaml();
        aggregateMetric1.setName("agg1");
        aggregateMetric1.setExpr("oji/ojc");
        Assert.assertEquals(
                new AggregateMetric.Divide(
                        new AggregateMetric.DocStats(new DocMetric.Field("OJI")),
                        new AggregateMetric.DocStats(new DocMetric.Field("OJC"))),
                metadataCache.parseMetric(aggregateMetric1));

        final MetricsYaml aggregateMetric2 = new MetricsYaml();
        aggregateMetric2.setName("agg2");
        aggregateMetric2.setExpr("(score-100)/4");
        Assert.assertEquals(
                new AggregateMetric.Divide(
                        new AggregateMetric.DocStats(new DocMetric.Subtract(new DocMetric.Field("SCORE"), new DocMetric.Constant(100))),
                        new AggregateMetric.Constant(4)),
                metadataCache.parseMetric(aggregateMetric2));

        final MetricsYaml overideMetric = new MetricsYaml();
        overideMetric.setName("o1");
        overideMetric.setExpr("o1+o2");
        Assert.assertEquals(
                new AggregateMetric.ImplicitDocStats(
                        new DocMetric.Add(new DocMetric.Field("O1"), new DocMetric.Field("O2"))),
                metadataCache.parseMetric(overideMetric));

        // won't do recursive expansion
        final MetricsYaml combinedMetric = new MetricsYaml();
        combinedMetric.setName("combined");
        combinedMetric.setExpr("same+complex");
        Assert.assertEquals(
                new AggregateMetric.ImplicitDocStats(new DocMetric.Add(new DocMetric.Field("SAME"), new DocMetric.Field("COMPLEX"))),
                metadataCache.parseMetric(combinedMetric));

        final MetricsYaml requireFTGSMetric = new MetricsYaml();
        requireFTGSMetric.setName("ftgsFunc");
        requireFTGSMetric.setExpr("PERCENTILE(oji, 95)");
        try {
            metadataCache.parseMetric(requireFTGSMetric);
            Assert.fail("require FTGS func is not supported");
        } catch (UnsupportedOperationException ex) {
        }
    }

    // for manual test: uncomment @Test
//    @Test
    public void testExistedDimension() throws URISyntaxException {
        final ImsClientInterface realIMSClient = ImsClient.build("***REMOVED***");
        final MetadataCache metadataCache = new MetadataCache(realIMSClient, null);
        // check if all existed dimensions can be parsed correctly
        metadataCache.updateMetadata();
        // validate all dimensions
        final Map<String, DatasetDimensions> uppercasedDimensions = metadataCache.getUppercasedDimensions();
        final DatasetsFields.Builder builder = DatasetsFields.builder();

        final DatasetYaml[] datasets = realIMSClient.getDatasets();
        for (DatasetYaml dataset : datasets) {
            if (Boolean.TRUE.equals(dataset.getDeprecated())) {
                continue;
            }
            for (FieldsYaml fieldsYaml : dataset.getFields()) {
                if (fieldsYaml == null || fieldsYaml.getType() == null) {
                    continue;
                }
                if (fieldsYaml.getType().equalsIgnoreCase("Integer")) {
                    builder.addIntField(dataset.getName(), fieldsYaml.getName());
                } else {
                    builder.addStringField(dataset.getName(), fieldsYaml.getName());
                }
                final DatasetDimensions datasetDimensions = uppercasedDimensions.get(dataset.getName().toUpperCase());
                for (String field : datasetDimensions.uppercasedFields()) {
                    final Dimension dimension = datasetDimensions.getDimension(field).get();
                    builder.addMetricField(dataset.getName(), dimension.name, dimension.isAlias);
                }
            }
        }

        final DatasetsFields datasetsFields = builder.build();
        List<String> errors = Lists.newArrayList();
        List<String> warnings = Lists.newArrayList();

        final Validator validator = new Validator() {

            @Override
            public void error(final String error) {
                errors.add(error);
            }

            @Override
            public void warn(final String warn) {
                warnings.add(warn);
            }
        };
        for (String dataset : datasetsFields.uppercasedDatasets()) {
            final DatasetDimensions dimensions = uppercasedDimensions.get(dataset);
            for (String field : dimensions.uppercasedFields()) {
                final Dimension dimension = dimensions.getDimension(field).get();
                dimension.metric.validate(
                        ImmutableSet.of(dataset),
                        datasetsFields, validator);
            }
        }

        System.out.printf("errors num: %d\n", errors.size());
        System.out.println(Joiner.on("\n").join(errors));

        System.out.printf("warnings num: %d\n", warnings.size());
        System.out.println(Joiner.on("\n").join(warnings));
    }
}