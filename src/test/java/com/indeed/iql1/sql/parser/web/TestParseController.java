package com.indeed.iql1.sql.parser.web;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableMap;
import com.indeed.iql.Constants;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.FieldFrequencyCache;
import com.indeed.iql2.IQL2Options;
import com.indeed.iql2.server.web.servlets.ParseServlet;
import com.indeed.iql2.server.web.servlets.QueryServletTestUtils;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import org.joda.time.DateTime;
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

    private static ParseServlet parseServlet;

    @BeforeClass
    public static void setUp() {
        final ImhotepMetadataCache cache = new ImhotepMetadataCache(
                null,
                AllData.DATASET.getNormalClient(),
                "",
                new FieldFrequencyCache(null)
        );
        cache.updateDatasets();
        parseServlet = new ParseServlet(cache, new IQL2Options());
    }

    private void testIQL1(final Map<String, Object> expected, final String query) throws IOException {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("Accept", "application/json");
        QueryServletTestUtils.LanguageVersion.ORIGINAL_IQL1.addRequestParameters(request);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        final String jsonResponse = OBJECT_MAPPER.writeValueAsString(parseServlet.parse(request, response, query));
        final String expectedJson = OBJECT_MAPPER.writeValueAsString(expected);
        Assert.assertEquals(expectedJson, jsonResponse);
    }

    @Test
    public void testBasic() throws IOException {
        final Map<String, Object> expected = ImmutableMap.of("parsed", true);

        testIQL1(
                expected
                , "from jobsearch 10d today group by time(1d) select count(), distinct(oji)"
        );
    }
}
