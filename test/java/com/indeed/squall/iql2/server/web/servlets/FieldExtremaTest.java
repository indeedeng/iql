package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FieldExtremaTest {
    @Test
    public void basicTest() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "3", "1000", "0", "15"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select FIELD_MIN(oji), FIELD_MAX(oji), FIELD_MIN(ojc), FIELD_MAX(ojc)");
    }
}
