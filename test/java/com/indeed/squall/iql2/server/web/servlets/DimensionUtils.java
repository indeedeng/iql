package com.indeed.squall.iql2.server.web.servlets;

import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.ims.client.DatasetInterface;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.ims.client.yamlFile.DatasetYaml;
import com.indeed.ims.client.yamlFile.MetricsYaml;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

            final MetricsYaml aliasI1 = new MetricsYaml();
            aliasI1.setName("aliasi1");
            aliasI1.setExpr("i1");
            metrics.add(aliasI1);

            final MetricsYaml aliasI2 = new MetricsYaml();
            aliasI2.setName("aliasi2");
            aliasI2.setExpr("i2");
            metrics.add(aliasI2);

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

        @Override
        public Set<String> getKeywordAnalyzerWhitelist(final String s) {
            return new HashSet<>();
        }

        @Override
        public Map<String, Set<String>> getWhitelist() {
            return new HashMap<>();
        }
    }


    public static Dataset createDataset() {
        final DateTimeZone timeZone = DateTimeZone.forOffsetHours(-6);
        final List<Dataset.DatasetShard> shards = new ArrayList<>();

        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 12, timeZone), 0, 0, "", "0.1"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 13, timeZone), 0, 2, "", "0.2"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 1, 0, 15, timeZone), 3, 2, "a", "0.3"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 2, 0, 30, timeZone), 3, 5, "b", "0.4"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 4, 0, 30, timeZone), 4, 1, "b", "1.0"));
            shards.add(new Dataset.DatasetShard("dimension", "index20150101.00", flamdex));
        }

        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 2, 2, 0, 30, timeZone), 4, 6, "b", "0.1"));
            shards.add(new Dataset.DatasetShard("dimension", "index20150102.00", flamdex));
        }

        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 2, 0, 30, timeZone), 0, 0, "a", "0.1"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 2, 2, 0, 30, timeZone), 4, -1, "b", "0.1"));
            shards.add(new Dataset.DatasetShard("dimension2", "index20150101.00", flamdex));
        }

        return new Dataset(shards);
    }

    private static FlamdexDocument makeDocument(DateTime timestamp, int i1, int i2, String s1, String f1) {
        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", timestamp.getMillis() / 1000);
        doc.addIntTerm("i1", i1);
        if (i2 != -1) {
            doc.addIntTerm("i2", i2);
        }
        doc.addStringTerm("s1", s1);
        doc.addStringTerm("floatf1", f1);
        doc.addIntTerm("empty", 0);
        doc.addIntTerm("same", 1);
        // TODO: This is a work-around for MemoryFlamdex not handling missing fields.
        doc.addIntTerm("fakeField", 0);
        return doc;
    }
}
