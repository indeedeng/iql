package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.iql.Constants;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author jwolfe
 */
public class CSVEscapeDataset {

    public static final String CRAZY_TERM = "Crazy,Term \"\n\t";

    static Dataset createDataset() {
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(Constants.DEFAULT_IQL_TIME_ZONE);

        final FlamdexDocument doc1 = new FlamdexDocument();
        doc1.addStringTerm("sField", CRAZY_TERM);
        doc1.addIntTerm("iField", 3);
        doc1.addIntTerm("unixtime", DateTime.parse("2015-01-01", dateTimeFormatter).getMillis() / 1000);
        flamdex.addDocument(doc1);

        final FlamdexDocument doc2 = new FlamdexDocument();
        doc2.addStringTerm("sField", "NormalTerm");
        doc2.addIntTerm("iField", 2);
        doc2.addIntTerm("unixtime", DateTime.parse("2015-01-01", dateTimeFormatter).getMillis() / 1000);
        flamdex.addDocument(doc2);

        return new Dataset(ImmutableList.of(new Dataset.DatasetShard("csvescape", "index20150101", flamdex)));
    }
}
