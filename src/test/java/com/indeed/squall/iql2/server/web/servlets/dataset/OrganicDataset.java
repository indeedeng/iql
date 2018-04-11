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

package com.indeed.squall.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;

public class OrganicDataset {
    public static Dataset create() {
        return create(false);
    }

    public static Dataset createWithDynamicShardNaming() {
        return create(true);
    }

    // Overall:
    // count = 151
    // oji = 2653
    // ojc = 306
    // distinct(tk) = { "a", "b", "c", "d" }, || = 4
    private static Dataset create(final boolean useDynamicShardNaming) {
        final DateTimeZone timeZone = DateTimeZone.forOffsetHours(-6);

        final List<Dataset.DatasetShard> result = new ArrayList<>();

        // 2015-01-01 00:00:00 - 2015-01-01 01:00:00
        // Random smattering of documents, including one 1ms before the shard ends.
        // total count = 10
        // total oji = 1180
        // total ojc = 45
        // distinct(tk) = { "a", "b", "c" }, || = 3
        // count(tk="a") = 4
        // count(tk="b") = 2
        // count(tk="c") = 4
        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            final DateTime start = new DateTime(2015, 1, 1, 0, 0, 0, timeZone);
            flamdex.addDocument(makeDocument(start.plusMillis(0), 10, 0, "a"));
            flamdex.addDocument(makeDocument(start.plusSeconds(30), 10, 1, "a"));
            flamdex.addDocument(makeDocument(start.plusMinutes(1).plusSeconds(15), 10, 5, "a"));
            flamdex.addDocument(makeDocument(start.plusMinutes(10), 10, 2, "b"));
            flamdex.addDocument(makeDocument(start.plusMinutes(15), 10, 1, "a"));
            flamdex.addDocument(makeDocument(start.plusMinutes(20), 100, 15, "b"));
            flamdex.addDocument(makeDocument(start.plusMinutes(25), 1000, 1, "c"));
            flamdex.addDocument(makeDocument(start.plusMinutes(30).plusSeconds(30), 10, 10, "c"));
            flamdex.addDocument(makeDocument(start.plusMinutes(45).plusSeconds(30), 10, 10, "c"));
            flamdex.addDocument(makeDocument(start.plusMinutes(59).plusSeconds(59).plusMillis(999), 10, 0, "c"));
            result.add(new Dataset.DatasetShard("organic", getShardId(useDynamicShardNaming, start, true), flamdex));
        }

        // 2015-01-01 01:00:00 - 2015-01-01 02:00:00
        // 1 document per minute with oji=10, ojc=1
        // total count = 60
        // total oji = 10 * 60 = 600
        // total ojc = 1 * 60 = 60
        // distinct(tk) = { "d" }, || = 1
        // count(tk="d") = 60
        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            final DateTime start = new DateTime(2015, 1, 1, 1, 0, 0, timeZone);
            for (int i = 0; i < 60; i++) {
                flamdex.addDocument(makeDocument(start.plusMinutes(i), 10, 1, "d"));
            }
            result.add(new Dataset.DatasetShard("organic", getShardId(useDynamicShardNaming, start, true), flamdex));
        }

        // 2015-01-01 02:00:00 - 03:00:00
        // 1 document per minute with oji=10, ojc=3
        // total count = 60
        // total oji = 10 * 60 = 600
        // total ojc = 3 * 60 = 180
        // distinct(tk) = { "d" }, || = 1
        // count(tk="d") = 60
        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            final DateTime start = new DateTime(2015, 1, 1, 2, 0, 0, timeZone);
            for (int i = 0; i < 60; i++) {
                flamdex.addDocument(makeDocument(start.plusMinutes(i), 10, 3, "d"));
            }
            result.add(new Dataset.DatasetShard("organic", getShardId(useDynamicShardNaming, start, true), flamdex));
        }

        // 1 document per hour from 2015-01-01 03:00:00 to 2015-01-02 00:00:00
        // oji = the hour
        // ojc = 1
        // count(tk="d") = 1
        // total count = 21
        // total oji = sum [3 .. 23] = 273
        // total ojc = 21
        // distinct(tk) = { "d" }, || = 1
        // total count(tk="d") = 21

        for (int h = 3; h < 24; h++) {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            final DateTime start = new DateTime(2015, 1, 1, h, 0, 0, timeZone);
            flamdex.addDocument(makeDocument(start, h, 1, "d"));
            result.add(new Dataset.DatasetShard("organic", getShardId(useDynamicShardNaming, start, true), flamdex));
        }

        for (int d = 1; d <= 31; d++) {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            final DateTime start = new DateTime(2014, 12, d, 0, 0, 0, timeZone);
            flamdex.addDocument(makeDocument(start, 1, 1, "d"));
            result.add(new Dataset.DatasetShard("organic", getShardId(useDynamicShardNaming, start, false), flamdex));
        }

        return new Dataset(result);
    }

    private static String getShardId(final boolean useDynamicShardNaming, final DateTime start, final boolean isHourly) {
        if (useDynamicShardNaming) {
            final DateTime end = isHourly ? start.plusHours(1) : start.plusDays(1);
            return String.format(
                    "dindex%04d%02d%02d.%02d-%04d%02d%02d.%02d.0.1",
                    start.getYear(),
                    start.getMonthOfYear(),
                    start.getDayOfMonth(),
                    start.getHourOfDay(),
                    end.getYear(),
                    end.getMonthOfYear(),
                    end.getDayOfMonth(),
                    end.getHourOfDay()
            );
        } else if (isHourly) {
            return String.format(
                    "index%04d%02d%02d.%02d",
                    start.getYear(),
                    start.getMonthOfYear(),
                    start.getDayOfMonth(),
                    start.getHourOfDay()
            );
        } else {
            return String.format(
                    "index%04d%02d%02d",
                    start.getYear(),
                    start.getMonthOfYear(),
                    start.getDayOfMonth()
            );
        }
    }

    private static FlamdexDocument makeDocument(final DateTime timestamp, final int oji, final int ojc, final String tk) {
        if (!timestamp.getZone().equals(DateTimeZone.forOffsetHours(-6))) {
            throw new IllegalArgumentException("Bad timestamp timezone: " + timestamp.getZone());
        }
        if (ojc > oji) {
            throw new IllegalArgumentException("Can't have more clicks than impressions");
        }

        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", timestamp.getMillis() / 1000);
        doc.addIntTerm("oji", oji);
        doc.addIntTerm("ojc", ojc);
        doc.addIntTerm("allbit", 1);
        doc.addStringTerm("tk", tk);

        // TODO: This is a work-around for MemoryFlamdex not handling missing fields.
        doc.addIntTerm("fakeField", 0);

        return doc;
    }
}
