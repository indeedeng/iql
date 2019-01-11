package com.indeed.iql1.sql.parser.web;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.FieldFrequencyCache;
import com.indeed.iql1.sql.ast2.QueryParts;
import com.indeed.iql1.web.SplitterServlet;
import com.indeed.iql2.server.web.servlets.QueryServletTestUtils.LanguageVersion;
import com.indeed.iql2.server.web.servlets.SplitServlet;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;

public class TestSplitterServlet {
    private static final SplitterServlet SPLITTER_SERVLET;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    static {
        final ImhotepMetadataCache cache = new ImhotepMetadataCache(
                null,
                AllData.DATASET.getNormalClient(),
                "",
                new FieldFrequencyCache(null),
                true
        );
        cache.updateDatasets();
        SPLITTER_SERVLET = new SplitterServlet(new SplitServlet(cache));
    }

    private void testAll(final QueryParts expected) throws IOException {
        for (final LanguageVersion version : LanguageVersion.values()) {
            doTest(expected, version);
        }
    }

    private void doTest(final QueryParts expected, final LanguageVersion version) throws IOException {
        final String query = expected.toString();
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("Accept", "application/json");
        version.addRequestParameters(request);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        final String jsonResponse = OBJECT_MAPPER.writeValueAsString(SPLITTER_SERVLET.doGet(request, response, query));
        final QueryParts actual = OBJECT_MAPPER.readValue(jsonResponse, QueryParts.class);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testBasic() throws IOException {
        testAll(new QueryParts("organic 2d 1d", "", "", ""));
        testAll(new QueryParts("organic 2d 1d", "country=\"us\"", "tk", "count()"));
    }

    @Test
    public void testFromKeywords() throws IOException {
        testAll(new QueryParts("from 2d 1d", "", "", ""));
        testAll(new QueryParts("where 2d 1d", "", "", ""));
        testAll(new QueryParts("group 2d 1d", "", "", ""));
        // SELECT must be backquoted because select is our only reserved word.
        testAll(new QueryParts("`select` 2d 1d", "", "", ""));
        testAll(new QueryParts("limit 2d 1d", "", "", ""));
    }

    @Test
    public void testWhereKeywords() throws IOException {
        testAll(new QueryParts("keywords 2d 1d", "from=\"from\"", "", ""));
        testAll(new QueryParts("keywords 2d 1d", "where=\"where\"", "", ""));
        testAll(new QueryParts("keywords 2d 1d", "group=\"group\"", "", ""));
        // SELECT must be backquoted because select is our only reserved word.
        testAll(new QueryParts("keywords 2d 1d", "`select`=\"select\"", "", ""));
        testAll(new QueryParts("keywords 2d 1d", "limit=\"limit\"", "", ""));
    }

    @Test
    public void testGroupByKeywords() throws IOException {
        testAll(new QueryParts("keywords 2d 1d", "", "limit", ""));
        testAll(new QueryParts("keywords 2d 1d", "", "from", ""));
        testAll(new QueryParts("keywords 2d 1d", "", "where", ""));
        testAll(new QueryParts("keywords 2d 1d", "", "group", ""));
        // SELECT must be backquoted because select is our only reserved word.
        testAll(new QueryParts("keywords 2d 1d", "", "`select`", ""));
    }

    @Test
    public void testSelectKeywords() throws IOException {
        testAll(new QueryParts("keywords 2d 1d", "", "", "from"));
        testAll(new QueryParts("keywords 2d 1d", "", "", "where"));
        testAll(new QueryParts("keywords 2d 1d", "", "", "group"));
        // SELECT must be backquoted because select is our only reserved word.
        testAll(new QueryParts("keywords 2d 1d", "", "", "`select`"));
        testAll(new QueryParts("keywords 2d 1d", "", "", "limit"));
    }
}
