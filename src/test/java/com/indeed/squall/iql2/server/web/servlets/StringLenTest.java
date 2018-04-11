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
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class StringLenTest extends BasicTest {

    @Test
    public void testStringLen() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "350")); //"Hi".length() * 50 + "Hello".length() * 50
        QueryServletTestUtils.testIQL2(MultiValuedDataset.create(), expected,
                "from dataset yesterday today select len(strField)");
    }

    @Test
    public void testStringLenGroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "100"));//"Hi".length() * 50
        expected.add(ImmutableList.of("1", "250"));//"Hello".length() * 50
        QueryServletTestUtils.testIQL2(MultiValuedDataset.create(), expected,
                "from dataset yesterday today group by groupId select len(strField)", true);
    }

    private static class MultiValuedDataset {
        public static Dataset create() {
            final List<Dataset.DatasetShard> shards = new ArrayList<>();
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            for (int i = 0; i < 100; i++) {
                flamdex.addDocument(
                        new FlamdexDocument.Builder()
                                .addIntTerm("id", i)
                                .addIntTerm("groupId", i % 2)
                                .addStringTerm("strField", i % 2 == 0 ? "Hi" : "Hello")
                                .build()
                );
            }

            shards.add(new Dataset.DatasetShard("dataset", "index20150101", flamdex));

            return new Dataset(shards);
        }
    }
}
