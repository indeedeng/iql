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

import java.util.ArrayList;
import java.util.List;

public class FloatScaleTest extends BasicTest {

    @Test
    public void testBasic() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "100", "100", "100001", "100001", "999100"));
        QueryServletTestUtils.testAll(createDataset(), expected,
                "from floatscaletest yesterday today select floatscale(field1, 1, 0), floatscale(field1), floatscale(field1, 1000, 0), floatscale(field1, 1000), floatscale(field2, 1, -500)");
    }

    static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = Lists.newArrayList();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();

        final FlamdexDocument doc1 = new FlamdexDocument();
        doc1.addStringTerm("field1", "100.0");
        doc1.addStringTerm("field2", "100.000000");
        flamdex.addDocument(doc1);

        final FlamdexDocument doc2 = new FlamdexDocument();
        doc2.addStringTerm("field1", "0.001");
        doc2.addStringTerm("field2", "1000000");
        flamdex.addDocument(doc2);
        shards.add(new Dataset.DatasetShard("floatscaletest", "index20150101", flamdex));
        return new Dataset(shards);
    }
}
