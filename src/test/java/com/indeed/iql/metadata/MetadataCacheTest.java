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

package com.indeed.iql.metadata;

import com.indeed.ims.client.yamlFile.MetricsYaml;
import com.indeed.iql.web.FieldFrequencyCache;
import org.junit.Assert;
import org.junit.Test;

public class MetadataCacheTest {

    @Test
    public void testParseDataset() {
        final ImhotepMetadataCache metadataCache = new ImhotepMetadataCache(null, null, "", new FieldFrequencyCache(null), true);
        final MetricsYaml calcMetric = new MetricsYaml();
        calcMetric.setName("complex");
        calcMetric.setExpr("(a1+a2)*10");
        final MetricMetadata dimensions = metadataCache.getMetricMetadataFromMetricsYaml(calcMetric, "test");
        Assert.assertEquals("(a1+a2)*10", dimensions.getExpression());
    }
}