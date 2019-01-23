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
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 0, timeZone), "abc", "us", 1, true));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 30, timeZone), "abd", "us", 1, false));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 1, 15, timeZone), "bcd", "us", 2, true));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 10, 0, timeZone), "abc", "uk", 2, false));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 15, 0, timeZone), "xyz", "uk", 3, true));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 20, 0, timeZone), "pqr", "jp", 3, false));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 25, 0, timeZone), "xyz", "jp", 4, true));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 30, 30, timeZone), "abc", "gb", 4, false));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 45, 30, timeZone), "bcd",  "gb", 5, true ));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 59, 59, 999, timeZone), "ddd", "cn", 5, false));
            result.add(new Dataset.DatasetShard("jobsearch", "index20150101.00", flamdex));
        }

        return new Dataset(result);
    }

    private static FlamdexDocument makeDocument(DateTime timestamp, String ctkrcvd, String country, int page, boolean isLast) {
        if (!timestamp.getZone().equals(DateTimeZone.forOffsetHours(-6))) {
            throw new IllegalArgumentException("Bad timestamp timezone: " + timestamp.getZone());
        }

        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", timestamp.getMillis() / 1000);
        doc.addStringTerm("country", country);
        doc.addStringTerm("ctkrcvd", ctkrcvd);

        // Nonsense terms for PrettyPrintTest
        doc.addStringTerm("escaped", "");
        doc.addIntTerm("oji", 10);
        doc.addIntTerm("ojc", 5);
        doc.addStringTerm("page", String.valueOf(page));
        if (isLast) {
            doc.addStringTerm("page", "last");
        }

        // TODO: This is a work-around for MemoryFlamdex not handling missing fields.
        doc.addIntTerm("fakeField", 0);

        return doc;
    }
}
