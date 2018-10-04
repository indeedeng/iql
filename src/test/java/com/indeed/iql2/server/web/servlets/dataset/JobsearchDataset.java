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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yuanlei
 */

public class JobsearchDataset {
    static Dataset create() {
        final DateTimeZone timeZone = DateTimeZone.forOffsetHours(-6);

        final List<Dataset.DatasetShard> result = new ArrayList<>();

        // 2015-01-01 00:00:00 - 2015-01-01 01:00:00
        // Random smattering of documents, including one 1ms before the shard ends.
        // total count = 10
        // distinct(country) = { "us", "uk", "jp", "gb", "cn"}, ||=5
        // distinct(ctkrcvd) = {"abc", "abd", "bcd", "xyz", "pqr", "ddd"}, ||=6
        // count(country="us") = 3
        // count(country="uk") = 2
        // count(country="jp") = 2
        // count(country="gb") = 2
        // count(country="cn") = 1
        // count(ctkrcvd="abc") = 3
        // count(ctkrcvd="abd") = 1
        // count(ctkrcvd="bcd") = 2
        // count(ctkrcvd="xyz") = 2
        // count(ctkrcvd="pqr") = 1
        // count(ctkrcvd="ddd") = 1
        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 0, timeZone), "abc", "us"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 30, timeZone), "abd", "us"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 1, 15, timeZone), "bcd", "us"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 10, 0, timeZone), "abc", "uk"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 15, 0, timeZone), "xyz", "uk"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 20, 0, timeZone), "pqr", "jp"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 25, 0, timeZone), "xyz", "jp"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 30, 30, timeZone), "abc", "gb"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 45, 30, timeZone), "bcd",  "gb"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 59, 59, 999, timeZone), "ddd", "cn"));
            result.add(new Dataset.DatasetShard("jobsearch", "index20150101.00", flamdex));
        }

        return new Dataset(result);
    }

    private static FlamdexDocument makeDocument(DateTime timestamp, String ctkrcvd, String country) {
        if (!timestamp.getZone().equals(DateTimeZone.forOffsetHours(-6))) {
            throw new IllegalArgumentException("Bad timestamp timezone: " + timestamp.getZone());
        }

        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", timestamp.getMillis() / 1000);
        doc.addStringTerm("country", country);
        doc.addStringTerm("ctkrcvd", ctkrcvd);

        // TODO: This is a work-around for MemoryFlamdex not handling missing fields.
        doc.addIntTerm("fakeField", 0);

        return doc;
    }
}
