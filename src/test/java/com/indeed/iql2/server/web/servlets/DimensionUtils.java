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

package com.indeed.iql2.server.web.servlets;

import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.ims.client.DatasetInterface;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.ims.client.yamlFile.DatasetYaml;
import com.indeed.ims.client.yamlFile.MetricsYaml;
import com.indeed.iql2.server.web.servlets.dataset.Dataset;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class DimensionUtils {

    public static class ImsClient implements ImsClientInterface {

        private final DatasetYaml[] datasets;

        public ImsClient() {
            datasets = createDatasets();
        }

        private DatasetYaml createDimension2() {
            List<MetricsYaml> metrics = new ArrayList<>();

            final MetricsYaml aliasi2 = new MetricsYaml();
            aliasi2.setName("i2");
            aliasi2.setExpr("i1");
            metrics.add(aliasi2);

            final MetricsYaml aliasCalc = new MetricsYaml();
            aliasCalc.setName("calc");
            aliasCalc.setExpr("(i1+i2)*10");
            metrics.add(aliasCalc);

            DatasetYaml dataset = new DatasetYaml();
            dataset.setName("dimension2");
            dataset.setType("Imhotep");
            dataset.setDescription("dimension2 dataset");
            dataset.setMetrics(metrics.toArray(new MetricsYaml[metrics.size()]));
            return dataset;
        }

        private DatasetYaml createDimension() {
            List<MetricsYaml> metrics = new ArrayList<>();

            final MetricsYaml emptyMetric = new MetricsYaml();
            emptyMetric.setName("empty");
            metrics.add(emptyMetric);

            final MetricsYaml sameMetric = new MetricsYaml();
            sameMetric.setName("same");
            sameMetric.setExpr("same");
            metrics.add(sameMetric);

            final MetricsYaml plusMetric = new MetricsYaml();
            plusMetric.setName("plus");
            plusMetric.setExpr("i1+i2");
            metrics.add(plusMetric);

            final MetricsYaml calcMetric = new MetricsYaml();
            calcMetric.setName("calc");
            calcMetric.setExpr("(i1+i2)*10");
            metrics.add(calcMetric);

            final MetricsYaml i3 = new MetricsYaml();
            i3.setName("i3");
            i3.setExpr("i2+i3");

            final MetricsYaml aliasI1 = new MetricsYaml();
            aliasI1.setName("aliasi1");
            aliasI1.setExpr("i1");
            metrics.add(aliasI1);

            final MetricsYaml aliasI2 = new MetricsYaml();
            aliasI2.setName("aliasi2");
            aliasI2.setExpr("i2");
            metrics.add(aliasI2);

            final MetricsYaml aliassi1 = new MetricsYaml();
            aliassi1.setName("si1");
            aliassi1.setExpr("si1");

            final MetricsYaml funcMetric = new MetricsYaml();
            funcMetric.setName("floatf1");
            funcMetric.setExpr("FLOATSCALE(floatf1, 10, 10)");
            metrics.add(funcMetric);

            final MetricsYaml aggMetric1 = new MetricsYaml();
            aggMetric1.setName("i1divi2");
            aggMetric1.setExpr("i1/i2");
            metrics.add(aggMetric1);

            DatasetYaml dimensionDataset = new DatasetYaml();
            dimensionDataset.setName("dimension");
            dimensionDataset.setType("Imhotep");
            dimensionDataset.setDescription("dimension dataset");
            dimensionDataset.setMetrics(metrics.toArray(new MetricsYaml[metrics.size()]));
            return dimensionDataset;
        }

        private DatasetYaml[] createDatasets() {
            return new DatasetYaml[]{createDimension(), createDimension2()};
        }

        @Override
        public DatasetInterface getDataset(final String s) {
            throw new UnsupportedOperationException("You need to implement this");
        }

        @Override
        public DatasetYaml[] getDatasets() {
            return datasets;
        }
    }
}
