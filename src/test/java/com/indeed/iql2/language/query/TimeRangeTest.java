package com.indeed.iql2.language.query;

import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.query.shardresolution.NullShardResolver;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.logging.TracingTreeTimer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TimeRangeTest {
    public static final DatasetsMetadata DATASETS_METADATA = AllData.DATASET.getDatasetsMetadata();
    public static final DateTimeZone ZONE = DateTimeZone.forOffsetHours(-6);
    public static final DateTime STOPPED_TIME = new DateTime(2015, 1, 1, 0, 0, ZONE);

    private void testStartEnd(final String query, final DateTime start, final DateTime end) {
        testStartEnd(query, start, end, false);
    }

    private void testStartEnd(final String query, final DateTime start, final DateTime end, final boolean useLegacy) {
        final TracingTreeTimer timer = new TracingTreeTimer();
        final Queries.ParseResult result = Queries.parseQuery(query, useLegacy, DATASETS_METADATA, Collections.emptySet(), new StoppedClock(STOPPED_TIME.getMillis()), timer, new NullShardResolver());
        for (final Dataset dataset : result.query.datasets) {
            Assert.assertEquals("Start doesn't match", start, dataset.startInclusive.unwrap());
            Assert.assertEquals("End doesn't match", end, dataset.endExclusive.unwrap());
        }
    }

    @Test
    public void testWeek() {
        testStartEnd(
                "from organic 1w today",
                new DateTime(2014, 12, 25, 0, 0, ZONE),
                new DateTime(2015, 1, 1, 0, 0, ZONE)
        );
        testStartEnd(
                "from organic \"1w\" today",
                new DateTime(2014, 12, 25, 0, 0, ZONE),
                new DateTime(2015, 1, 1, 0, 0, ZONE)
        );
        testStartEnd(
                "from organic w today",
                new DateTime(2014, 12, 25, 0, 0, ZONE),
                new DateTime(2015, 1, 1, 0, 0, ZONE)
        );
        testStartEnd(
                "from organic \"w\" today",
                new DateTime(2014, 12, 25, 0, 0, ZONE),
                new DateTime(2015, 1, 1, 0, 0, ZONE)
        );

    }

    @Test
    public void testYear() {
        testStartEnd(
                "from organic 1y today",
                new DateTime(2014, 1, 1, 0, 0, ZONE),
                new DateTime(2015, 1, 1, 0, 0, ZONE)
        );
        testStartEnd(
                "from organic \"1y\" today",
                new DateTime(2014, 1, 1, 0, 0, ZONE),
                new DateTime(2015, 1, 1, 0, 0, ZONE)
        );
    }

    @Test
    public void testYesterday() {
        testStartEnd(
                "from organic y today",
                new DateTime(2014, 12, 31, 0, 0, ZONE),
                new DateTime(2015, 1, 1, 0, 0, ZONE)
        );
        testStartEnd(
                "from organic \"y\" today",
                new DateTime(2014, 12, 31, 0, 0, ZONE),
                new DateTime(2015, 1, 1, 0, 0, ZONE)
        );
        testStartEnd(
                "from organic 1d today",
                new DateTime(2014, 12, 31, 0, 0, ZONE),
                new DateTime(2015, 1, 1, 0, 0, ZONE)
        );
        testStartEnd(
                "from organic \"1d\" today",
                new DateTime(2014, 12, 31, 0, 0, ZONE),
                new DateTime(2015, 1, 1, 0, 0, ZONE)
        );
        for (final String today : new String[]{"t", "to", "tod", "toda", "today"}) {
            testStartEnd(
                    "from organic 1d " + today,
                    new DateTime(2014, 12, 31, 0, 0, ZONE),
                    new DateTime(2015, 1, 1, 0, 0, ZONE),
                    true
            );
        }
        testStartEnd(
                "from organic d today",
                new DateTime(2014, 12, 31, 0, 0, ZONE),
                new DateTime(2015, 1, 1, 0, 0, ZONE)
        );
        testStartEnd(
                "from organic \"d\" today",
                new DateTime(2014, 12, 31, 0, 0, ZONE),
                new DateTime(2015, 1, 1, 0, 0, ZONE)
        );
    }
}
