package com.indeed.iql2.server.web.servlets;

import com.indeed.ims.client.DatasetInterface;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.ims.client.yamlFile.DatasetYaml;
import org.junit.Test;

import java.util.Collections;

public class DeprecatedDatasetWarningTest {
    private static final DeprecatedImsClient DEPRECATED_IMS_CLIENT = new DeprecatedImsClient();
    public static final QueryServletTestUtils.Options OPTIONS = QueryServletTestUtils.Options.create().setImsClient(DEPRECATED_IMS_CLIENT);

    @Test
    public void testDeprecatedFlag() throws Exception {
        QueryServletTestUtils.testWarning(
                Collections.singletonList("Dataset 'organic' is deprecated. Check the dataset description for alternative data sources."),
                "from organic yesterday today",
                OPTIONS
        );
    }

    @Test
    public void testDeprecatedDescription() throws Exception {
        QueryServletTestUtils.testWarning(
                Collections.singletonList("Dataset 'jobsearch' is deprecated. Check the dataset description for alternative data sources."),
                "from jobsearch 2015-01-01 2015-01-01T01:00:00",
                OPTIONS
        );
    }

    private static class DeprecatedImsClient implements ImsClientInterface {
        private final DatasetYaml[] datasets;

        public DeprecatedImsClient() {
            datasets = new DatasetYaml[]{deprecatedOrganic(), deprecatedJobsearchDescription()};
        }

        private DatasetYaml deprecatedOrganic() {
            final DatasetYaml dataset = new DatasetYaml();
            dataset.setName("organic");
            dataset.setDeprecated(true);
            return dataset;
        }

        private DatasetYaml deprecatedJobsearchDescription() {
            final DatasetYaml dataset = new DatasetYaml();
            dataset.setName("jobsearch");
            dataset.setDescription("Jobsearch is a deprecated index! Don't use it!");
            return dataset;
        }

        @Override
        public DatasetInterface getDataset(final String s) {
            throw new UnsupportedOperationException("Unused in test and unimplemented");
        }

        @Override
        public DatasetYaml[] getDatasets() {
            return datasets;
        }
    }
}
