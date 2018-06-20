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

package com.indeed.squall.iql2.server.web.metadata;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.web.FieldFrequencyCache;
import com.indeed.ims.client.ImsClient;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.ims.client.yamlFile.DatasetYaml;
import com.indeed.ims.client.yamlFile.MetricsYaml;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.dimensions.Dimension;
import com.indeed.squall.iql2.language.util.ValidationHelper;
import com.indeed.squall.iql2.language.metadata.DatasetsMetadata;
import org.junit.Assert;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MetadataCacheTest {
    @Test
    public void testParseMetric() {
        final MetadataCache metadataCache = new MetadataCache(null, null, new FieldFrequencyCache(null));

        final List<String> options = Collections.emptyList();

        final MetricsYaml countsMetric = new MetricsYaml();
        countsMetric.setName("counts");
        countsMetric.setExpr("count()");
        Assert.assertEquals(new AggregateMetric.DocStats(new DocMetric.Count()),
                metadataCache.parseMetric(countsMetric.getName(), countsMetric.getExpr(), options));

        final MetricsYaml sameMetric = new MetricsYaml();
        sameMetric.setName("same");
        sameMetric.setExpr("same");
        Assert.assertEquals(new AggregateMetric.DocStats(new DocMetric.Field("SAME")),
                metadataCache.parseMetric(sameMetric.getName(), sameMetric.getExpr(), options));

        final MetricsYaml calcMetric = new MetricsYaml();
        calcMetric.setName("complex");
        calcMetric.setExpr("(a1+a2)*10");
        Assert.assertEquals(
                new AggregateMetric.DocStats(
                        new DocMetric.Multiply(new DocMetric.Add(new DocMetric.Field("A1"), new DocMetric.Field("A2")), new DocMetric.Constant(10))),
                metadataCache.parseMetric(calcMetric.getName(), calcMetric.getExpr(), options));

        final MetricsYaml aggregateMetric1 = new MetricsYaml();
        aggregateMetric1.setName("agg1");
        aggregateMetric1.setExpr("oji/ojc");
        Assert.assertEquals(
                new AggregateMetric.Divide(
                        new AggregateMetric.DocStats(new DocMetric.Field("OJI")),
                        new AggregateMetric.DocStats(new DocMetric.Field("OJC"))),
                metadataCache.parseMetric(aggregateMetric1.getName(), aggregateMetric1.getExpr(), null));

        final MetricsYaml aggregateMetric2 = new MetricsYaml();
        aggregateMetric2.setName("agg2");
        aggregateMetric2.setExpr("(score-100)/4");
        Assert.assertEquals(
                new AggregateMetric.Divide(
                        new AggregateMetric.DocStats(new DocMetric.Subtract(new DocMetric.Field("SCORE"), new DocMetric.Constant(100))),
                        new AggregateMetric.Constant(4)),
                metadataCache.parseMetric(aggregateMetric2.getName(), aggregateMetric2.getExpr(), options));

        final MetricsYaml overideMetric = new MetricsYaml();
        overideMetric.setName("o1");
        overideMetric.setExpr("o1+o2");
        Assert.assertEquals(
                new AggregateMetric.DocStats(
                        new DocMetric.Add(new DocMetric.Field("O1"), new DocMetric.Field("O2"))),
                metadataCache.parseMetric(overideMetric.getName(), overideMetric.getExpr(), options));

        // won't do recursive expansion
        final MetricsYaml combinedMetric = new MetricsYaml();
        combinedMetric.setName("combined");
        combinedMetric.setExpr("same+complex");
        Assert.assertEquals(
                new AggregateMetric.DocStats(new DocMetric.Add(new DocMetric.Field("SAME"), new DocMetric.Field("COMPLEX"))),
                metadataCache.parseMetric(combinedMetric.getName(), combinedMetric.getExpr(), options));

        final MetricsYaml requireFTGSMetric = new MetricsYaml();
        requireFTGSMetric.setName("ftgsFunc");
        requireFTGSMetric.setExpr("PERCENTILE(oji, 95)");
        try {
            metadataCache.parseMetric(requireFTGSMetric.getName(), requireFTGSMetric.getExpr(), options);
            Assert.fail("require FTGS func is not supported");
        } catch (UnsupportedOperationException ex) {
        }
    }

    // for manual test: uncomment @Test
//    @Test
    public void testExistedDimension() throws URISyntaxException {

        final ImhotepClient imhotepClient = new ImhotepClient("***REMOVED***",
                "/imhotep/interactive-daemons", true);
        final ImsClientInterface realIMSClient = ImsClient.build("***REMOVED***");
        final MetadataCache metadataCache = new MetadataCache(realIMSClient, imhotepClient, new FieldFrequencyCache(null));
        // check if all existed dimensions can be parsed correctly
        metadataCache.updateMetadata();
        // validate all dimensions
        final DatasetsMetadata datasetsMetadata = metadataCache.get();
        final ValidationHelper validationHelper = new ValidationHelper(datasetsMetadata, Collections.emptyMap(), Collections.emptyMap(), true);
        final List<String> errors = Lists.newArrayList();
        final List<String> warnings = Lists.newArrayList();

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
        for (String dataset : validationHelper.datasets()) {
            for (final Dimension dimension : datasetsMetadata.getMetadata(dataset).get().fieldToDimension.values()) {
                dimension.metric.validate(
                        ImmutableSet.of(dataset),
                        validationHelper, validator);
            }
        }

        System.out.printf("errors num: %d\n", errors.size());
        System.out.println(Joiner.on("\n").join(errors));

        System.out.printf("warnings num: %d\n", warnings.size());
        System.out.println(Joiner.on("\n").join(warnings));
    }

    @Test
    public void testParseDataset() {
        final MetadataCache metadataCache = new MetadataCache(null, null, new FieldFrequencyCache(null));
        final DatasetYaml imhotepDataset = new DatasetYaml();
        imhotepDataset.setName("imhotep");
        imhotepDataset.setType("Imhotep");
        final MetricsYaml calcMetric = new MetricsYaml();
        calcMetric.setName("complex");
        calcMetric.setExpr("(a1+a2)*10");
        imhotepDataset.setMetrics(new MetricsYaml[]{calcMetric});
        final ImmutableMap<String, Dimension> dimensions = metadataCache.buildDatasetDimension(imhotepDataset);

        final ImmutableMap<String, String> expectedDimensions = ImmutableMap.of(
                "complex", "(a1+a2)*10",
                "counts", "count()",
                "dayofweek", "(((unixtime-280800)%604800)\\86400)",
                "timeofday", "((unixtime-21600)%86400)");
        final Map<String, String> returnedDimensions = dimensions.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().expression));

        Assert.assertEquals(expectedDimensions, returnedDimensions);
    }
}