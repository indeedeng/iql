package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import org.junit.Test;

import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testWarning;

/**
 *
 */
public class StringAsIntFieldTest {
    final Dataset dataset = createStringAsIntDataset();

    @Test
    public void testSelectStringAsIntField() throws Exception {
        testWarning(dataset, ImmutableList.of("Field \"PAGE\" in Dataset \"DATASET\" is a string field but it is used as an int field in [HasInt{field='Positioned{t=PAGE}', term=0}]"),
                "from dataset yesterday today SELECT page > 0");
        testWarning(dataset, ImmutableList.of("Field \"PAGE\" in Dataset \"DATASET\" is a string field but it is used as an int field in [HasInt{field='Positioned{t=PAGE}', term=0}]"),
                "from dataset yesterday today SELECT page = 0");
        testWarning(dataset, ImmutableList.of("Field \"PAGE\" in Dataset \"DATASET\" is a string field but it is used as an int field in [HasInt{field='Positioned{t=PAGE}', term=0}]"),
                "from dataset yesterday today SELECT page != 0");

        testWarning(dataset, ImmutableList.of(), "from jobsearch yesterday today SELECT page = 0");
    }

    @Test
    public void testFilterStringAsIntField() throws Exception {
        testWarning(dataset, ImmutableList.of("Field \"PAGE\" in Dataset \"DATASET\" is a string field but it is used as an int field in " +
                        "[QueryAction{scope=[DATASET], perDatasetQuery={DATASET=int:PAGE:0}, targetGroup=1, positiveGroup=1, negativeGroup=0}]"),
                "from dataset yesterday today where page = 0");
        testWarning(dataset, ImmutableList.of("Field \"PAGE\" in Dataset \"DATASET\" is a string field but it is used as an int field in " +
                        "[QueryAction{scope=[DATASET], perDatasetQuery={DATASET=int:PAGE:0}, targetGroup=1, positiveGroup=0, negativeGroup=1}]"),
                "from dataset yesterday today where page != 0");

        testWarning(dataset, ImmutableList.of(), "from jobsearch yesterday today where page = 0");
    }



    public static Dataset createStringAsIntDataset() {
        final List<Dataset.DatasetShard> shards = Lists.newArrayList();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        for (int i = 0; i < 100; i++) {
            flamdex.addDocument(
                    new FlamdexDocument.Builder()
                            .addIntTerm("id", i)
                            .addStringTerm("page", ((i % 2) == 0) ? "0" : "1")
                            .build()
            );
        }

        shards.add(new Dataset.DatasetShard("dataset", "index20150101", flamdex));
        shards.add(new Dataset.DatasetShard("jobsearch", "index20150101", flamdex));
        return new Dataset(shards);
    }
}
