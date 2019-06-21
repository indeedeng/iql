package com.indeed.iql1.sql.parser.web;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.iql.Constants;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.FieldFrequencyCache;
import com.indeed.iql1.web.ParseController;
import com.indeed.iql2.IQL2Options;
import com.indeed.iql2.server.web.servlets.ParseServlet;
import com.indeed.iql2.server.web.servlets.QueryServletTestUtils;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;
import java.util.Map;

public class TestParseController {
    private static final DateTime NOW = DateTime.now().withZone(Constants.DEFAULT_IQL_TIME_ZONE);
    private static final WallClock NOW_CLOCK = new StoppedClock(NOW.getMillis());
    private static final DateTime TODAY = NOW.withMillisOfDay(0);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, true)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, true);

    private static ParseController parseController;

    @BeforeClass
    public static void setUp() {
        final ImhotepMetadataCache cache = new ImhotepMetadataCache(
                null,
                AllData.DATASET.getNormalClient(),
                "",
                new FieldFrequencyCache(null)
        );
        cache.updateDatasets();
        parseController = new ParseController(cache, new ParseServlet(cache, new IQL2Options()), NOW_CLOCK);
    }

    private void testIQL1(final Map<String, Object> expected, final String query) throws IOException {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("Accept", "application/json");
        QueryServletTestUtils.LanguageVersion.ORIGINAL_IQL1.addRequestParameters(request);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        final String jsonResponse = OBJECT_MAPPER.writeValueAsString(parseController.handleParse(query, "1", request, response));
        final String expectedJson = OBJECT_MAPPER.writeValueAsString(expected);
        Assert.assertEquals(expectedJson, jsonResponse);
    }

    @Test
    public void testBasic() throws IOException {
        final Map<String, Object> expected = ImmutableMap.of(
                "from", ImmutableMap.of(
                        "dataset", "jobsearch",
                        "start", TODAY.minusDays(10),
                        "end", TODAY,
                        "startRawString", "10d",
                        "endRawString", "today"
                ),
                "where", ImmutableMap.of(
                        "expression", ImmutableMap.of(
                                "operator", "AND",
                                "left", ImmutableMap.of(
                                        "operator", "EQ",
                                        "left", ImmutableMap.of("name", "rcv"),
                                        "right", ImmutableMap.of("name", "jsv")
                                ),
                                "right", ImmutableMap.of(
                                        "operator", "NOT_EQ",
                                        "left", ImmutableMap.of("name", "grp"),
                                        "right", ImmutableMap.of("string", "spider")
                                )
                        )
                ),
                "groupBy", ImmutableMap.of(
                        "groupings", ImmutableList.of(
                                ImmutableMap.of(
                                        "function", "time",
                                        "args", ImmutableList.of(ImmutableMap.of("string", "1d")))
                        )
                ),
                "limit", Integer.MAX_VALUE - 1,
                "select", ImmutableMap.of(
                        "projections", ImmutableList.of(
                                ImmutableMap.of(
                                        "function", "count",
                                        "args", ImmutableList.of()
                                ),
                                ImmutableMap.of(
                                        "function", "distinct",
                                        "args", ImmutableList.of(ImmutableMap.of("name", "jobid"))
                                )
                        )
                )
        );

        testIQL1(
                expected
                , "from jobsearch 10d today where rcv=jsv grp!='spider' group by time(1d) select count(), distinct(jobid)"
        );
    }
}
