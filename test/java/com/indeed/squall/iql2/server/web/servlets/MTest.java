package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MTest extends BasicTest {
    @Test
    public void test() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151", "1", "0", "151", "0", "4", "2", "1", "0", "6", "1"));
        QueryServletTestUtils.testIQL2(
                OrganicDataset.create(),
                expected,
                "from organic yesterday today select count(), m(true), m(false), [m(true)], [m(false)], [m(tk='a')], [m(tk='b')], m(count()=151), [m(tk='a' and tk='b')], [m(tk='a' or tk='b')], m(count()>1 and tk='a'>0)"
        );
    }
}
