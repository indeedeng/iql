package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.iql.Constants;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */
public class DatasetGroupByDataset {
    private static final DateTimeZone DATE_TIME_ZONE = Constants.DEFAULT_IQL_TIME_ZONE;
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyyMMdd").withZone(DATE_TIME_ZONE);
    private static final String DATASET = "groupByDataset";

    private static Dataset.DatasetShard makeShard(LocalDate day, int count) {
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        final FlamdexDocument document = new FlamdexDocument.Builder().addIntTerm("label", 1).addIntTerm("fakeField", 0).addIntTerm("unixtime", day.toDateTimeAtStartOfDay(DATE_TIME_ZONE).getMillis() / 1000).build();
        for (int i = 0; i < count / 2; i++) {
            flamdex.addDocument(document);
        }
        final FlamdexDocument document2 = new FlamdexDocument.Builder().addIntTerm("label", 2).addIntTerm("fakeField", 0).addIntTerm("unixtime", day.toDateTimeAtStartOfDay(DATE_TIME_ZONE).getMillis() / 1000).build();
        for (int i = 0; i < count / 2; i++) {
            flamdex.addDocument(document2);
        }
        return new Dataset.DatasetShard(DATASET, "index" + FORMATTER.print(day), flamdex);
    }

    /**
     * 2015-01-01: 100
     * 2015-01-02: 200
     * 2015-01-03: 300
     * 2015-01-04: 400
     * 2015-01-05: 500
     * 2015-01-06: 600
     * 2015-01-07: 700
     * 2015-01-08: 1000
     * 2015-01-09: 2000
     * 2015-01-10: 3000
     * 2015-01-11: 4000
     * 2015-01-12: 5000
     * 2015-01-13: 6000
     * 2015-01-14: 7000
     */
    static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            shards.add(makeShard(new LocalDate(2015, 1, 1 + i), 100 * (i + 1)));
            shards.add(makeShard(new LocalDate(2015, 1, 8 + i), 1000 * (i + 1)));
            shards.add(makeShard(new LocalDate(2015, 1, 15 + i), 10000 * (i + 1)));
        }
        return new Dataset(shards);
    }
}
