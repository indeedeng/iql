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
                "from dataset yesterday today SELECT page = 0", QueryServletTestUtils.LanguageVersion.IQL2);
        testWarning(dataset, ImmutableList.of("Field \"VP\" in Dataset \"DATASET\" is a string field but it is used as an int field in [HasInt{field='Positioned{t=VP}', term=0}]"),
                "from dataset yesterday today SELECT vp != 0", QueryServletTestUtils.LanguageVersion.IQL2);

        testWarning(dataset, ImmutableList.of(), "from dataset yesterday today SELECT vp != 0", QueryServletTestUtils.LanguageVersion.IQL1);
        testWarning(dataset, ImmutableList.of(), "from jobsearch yesterday today SELECT page = 0");
    }

    @Test
    public void testFilterStringAsIntField() throws Exception {
        testWarning(dataset, ImmutableList.of("Field \"PAGE\" in Dataset \"DATASET\" is a string field but it is used as an int field in " +
                        "[QueryAction{scope=[DATASET], perDatasetQuery={DATASET=int:PAGE:0}, targetGroup=1, positiveGroup=1, negativeGroup=0}]"),
                "from dataset yesterday today where page = 0", QueryServletTestUtils.LanguageVersion.IQL2);
        testWarning(dataset, ImmutableList.of("Field \"VP\" in Dataset \"DATASET\" is a string field but it is used as an int field in " +
                        "[QueryAction{scope=[DATASET], perDatasetQuery={DATASET=int:VP:0}, targetGroup=1, positiveGroup=0, negativeGroup=1}]"),
                "from dataset yesterday today where vp != 0", QueryServletTestUtils.LanguageVersion.IQL2);
        testWarning(dataset, ImmutableList.of("Field \"VP\" in Dataset \"DATASET\" is a string field but it is used as an int field in " +
                        "[IntOrAction{scope=[DATASET], field='VP', terms=[2, 3, 1], targetGroup=1, positiveGroup=1, negativeGroup=0}]"),
                "from dataset yesterday today where vp in (1, 2, 3)", QueryServletTestUtils.LanguageVersion.IQL2);

        testWarning(dataset, ImmutableList.of(), "from dataset yesterday today where vp != 0", QueryServletTestUtils.LanguageVersion.IQL1);
        testWarning(dataset, ImmutableList.of(), "from dataset yesterday today where vp in (1, 2, 3)", QueryServletTestUtils.LanguageVersion.IQL1);
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
                            .addStringTerm("vp", ((i % 2) == 0) ? "0" : "1")
                            .build()
            );
        }

        shards.add(new Dataset.DatasetShard("dataset", "index20150101", flamdex));
        shards.add(new Dataset.DatasetShard("jobsearch", "index20150101", flamdex));
        return new Dataset(shards);
    }
}
