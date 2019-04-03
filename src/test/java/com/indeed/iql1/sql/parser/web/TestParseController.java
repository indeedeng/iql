package com.indeed.iql1.sql.parser.web;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.FieldFrequencyCache;
import com.indeed.iql1.sql.ast.BinaryExpression;
import com.indeed.iql1.sql.ast.FunctionExpression;
import com.indeed.iql1.sql.ast.NameExpression;
import com.indeed.iql1.sql.ast.Op;
import com.indeed.iql1.sql.ast.StringExpression;
import com.indeed.iql1.sql.ast2.FromClause;
import com.indeed.iql1.sql.ast2.GroupByClause;
import com.indeed.iql1.sql.ast2.IQL1SelectStatement;
import com.indeed.iql1.sql.ast2.SelectClause;
import com.indeed.iql1.sql.ast2.WhereClause;
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

public class TestParseController {
    private static final DateTime NOW = DateTime.now().withZone(DateTimeZone.forOffsetHours(-6));
    private static final WallClock NOW_CLOCK = new StoppedClock(NOW.getMillis());
    private static final DateTime TODAY = NOW.withMillisOfDay(0);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, true)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, true)
            ;

    private static ParseController parseController;

    @BeforeClass
    public static void setUp() {
        final ImhotepMetadataCache cache = new ImhotepMetadataCache(
                null,
                AllData.DATASET.getNormalClient(),
                "",
                new FieldFrequencyCache(null),
                true
        );
        cache.updateDatasets();
        parseController = new ParseController(cache, new ParseServlet(cache, new IQL2Options()), NOW_CLOCK);
    }

    private void testIQL1(final IQL1SelectStatement expected, final String query) throws IOException {
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
        testIQL1(
                new IQL1SelectStatement(
                        new SelectClause(ImmutableList.of(
                                new FunctionExpression("count", ImmutableList.of()),
                                new FunctionExpression("distinct", ImmutableList.of(new NameExpression("jobid")))
                        )),
                        new FromClause("jobsearch", TODAY.minusDays(10), TODAY, "10d", "today"),
                        new WhereClause(
                                new BinaryExpression(
                                        new BinaryExpression(new NameExpression("rcv"), Op.EQ, new NameExpression("jsv")),
                                        Op.AND,
                                        new BinaryExpression(new NameExpression("group"), Op.NOT_EQ, new StringExpression("spider"))
                                )
                        ),
                        new GroupByClause(ImmutableList.of(new FunctionExpression("time", ImmutableList.of(new StringExpression("1d"))))),
                        Integer.MAX_VALUE - 1
                )
                , "from jobsearch 10d today where rcv=jsv group!='spider' group by time(1d) select count(), distinct(jobid)"
        );
    }
}
