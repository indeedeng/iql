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

package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.ArrayList;
import java.util.List;

public class BigDataset {
    static Dataset create() {
        final List<Dataset.DatasetShard> result = new ArrayList<>();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        for (int i = 0; i < 6_000; i++) {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("field", i);
            flamdex.addDocument(doc);
        }
        result.add(new Dataset.DatasetShard("big", "index20150101.00-20150102.00", flamdex));
        return new Dataset(result);
    }
}

