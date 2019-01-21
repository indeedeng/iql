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

package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AggregateMetricsTest extends BasicTest {
    @Test
    public void testLog() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        //log(100), log(151) 
        expected.add(ImmutableList.of("", String.valueOf(4.6051702), String.valueOf(5.0172798)));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select log(100), log(count())");
    }

    @Test
    public void testAbs() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "100.5", "100.5", "151"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select abs(100.5), abs(-100.5), abs(count())");
    }

    @Test
    public void testFloor() throws Exception {
        //floor(x, 0)
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "101", "101", "-106", "-106", "151")),
                "from organic yesterday today select floor(101.46), floor(101.64), floor(-105.46), floor(-105.64), floor(count())");

        //floor(x, 1)
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "101.4", "101.6", "-105.5", "-105.7", "151")),
                "from organic yesterday today select floor(101.46, 1), floor(101.64, 1), floor(-105.46, 1), floor(-105.64, 1), floor(count(), 1)");

        //floor(x, -1)
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "100", "100", "-110", "-110", "150")),
                "from organic yesterday today select floor(101.46, -1), floor(101.64, -1), floor(-105.46, -1), floor(-105.64, -1), floor(count(), -1)");

        //multi-ftgs
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(
                        ImmutableList.of("a", "5.7142857", "5.7", "0", "-5.8", "-10"),
                        ImmutableList.of("b", "6.4705882", "6.4", "0", "-6.5", "-10"),
                        ImmutableList.of("c", "49.047619", "49", "40", "-49.1", "-50"),
                        ImmutableList.of("d", "5.6436782", "5.6", "0", "-5.7", "-10")),
                "from organic yesterday today group by tk select oji/ojc, floor(oji/ojc, 1), floor(oji/ojc,-1), floor(-oji/ojc, 1), floor(-oji/ojc, -1)");

        //special cases
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "0", "0", "-1", "∞", "-∞", "NaN")),
                "from organic yesterday today select floor(0.0), floor(-0.0), floor(-0.3), floor(1.0/0.0), floor(-1.0/0.0), floor(0.0/0.0)");
    }

    @Test
    public void testFloorDigitsOverLimit() throws Exception {
        QueryServletTestUtils.expectException(
                AllData.DATASET,
                "from organic yesterday today select floor(101.46, 11)",
                QueryServletTestUtils.LanguageVersion.IQL2,
                errorMsg -> errorMsg.contains("The max digits for FLOOR is 10"));

        QueryServletTestUtils.expectException(
                AllData.DATASET,
                "from organic yesterday today select floor(101.46, -11)",
                QueryServletTestUtils.LanguageVersion.IQL2,
                errorMsg -> errorMsg.contains("The max digits for FLOOR is 10"));
    }

    @Test
    public void testCeil() throws Exception {
        //ceil(x, 0)
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "102", "102", "-105", "-105", "151")),
                "from organic yesterday today select ceil(101.46), ceil(101.64), ceil(-105.46), ceil(-105.64), ceil(count())");

        //ceil(x, 1)
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "101.5", "101.7", "-105.4", "-105.6", "151")),
                "from organic yesterday today select ceil(101.46, 1), ceil(101.64, 1), ceil(-105.46, 1), ceil(-105.64, 1), ceil(count(), 1)");

        //ceil(x, -1)
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "110", "110", "-100", "-100", "160")),
                "from organic yesterday today select ceil(101.46, -1), ceil(101.64, -1), ceil(-105.46, -1), ceil(-105.64, -1), ceil(count(), -1)");

        //multi-ftgs
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(
                        ImmutableList.of("a", "5.7142857", "5.8", "10", "-5.7", "0"),
                        ImmutableList.of("b", "6.4705882", "6.5", "10", "-6.4", "0"),
                        ImmutableList.of("c", "49.047619", "49.1", "50", "-49", "-40"),
                        ImmutableList.of("d", "5.6436782", "5.7", "10", "-5.6", "0")),
                "from organic yesterday today group by tk select oji/ojc, ceil(oji/ojc, 1), ceil(oji/ojc,-1), ceil(-oji/ojc, 1), ceil(-oji/ojc, -1)");

        //special cases
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "0", "0", "0", "∞", "-∞", "NaN")),
                "from organic yesterday today select ceil(0.0), ceil(-0.0), ceil(-0.3), ceil(1.0/0.0), ceil(-1.0/0.0), ceil(0.0/0.0)");
    }

    @Test
    public void testCeilDigitsOverLimit() throws Exception {
        QueryServletTestUtils.expectException(
                AllData.DATASET,
                "from organic yesterday today select ceil(101.46, 11)",
                QueryServletTestUtils.LanguageVersion.IQL2,
                errorMsg -> errorMsg.contains("The max digits for CEIL is 10"));

        QueryServletTestUtils.expectException(
                AllData.DATASET,
                "from organic yesterday today select ceil(101.46, -11)",
                QueryServletTestUtils.LanguageVersion.IQL2,
                errorMsg -> errorMsg.contains("The max digits for CEIL is 10"));
    }

    @Test
    public void testRound() throws Exception {
        //round(x, 0)
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "101", "102", "-105", "-106", "151")),
                "from organic yesterday today select round(101.46), round(101.64), round(-105.46), round(-105.64), round(count())");

        //round(x, 1)
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "101.5", "101.6", "-105.5", "-105.6", "151")),
                "from organic yesterday today select round(101.46, 1), round(101.64, 1), round(-105.46, 1), round(-105.64, 1), round(count(), 1)");

        //round(x, -1)
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "100", "100", "-110", "-110", "150")),
                "from organic yesterday today select round(101.46, -1), round(101.64, -1), round(-105.46, -1), round(-105.64, -1), round(count(), -1)");

        //multi-ftgs
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(
                        ImmutableList.of("a", "5.7142857", "5.7", "10", "-5.7", "-10"),
                        ImmutableList.of("b", "6.4705882", "6.5", "10", "-6.5", "-10"),
                        ImmutableList.of("c", "49.047619", "49", "50", "-49", "-50"),
                        ImmutableList.of("d", "5.6436782", "5.6", "10", "-5.6", "-10")),
                "from organic yesterday today group by tk select oji/ojc, round(oji/ojc, 1), round(oji/ojc,-1), round(-oji/ojc, 1), round(-oji/ojc, -1)");

        //special cases
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "0", "0", "0", "∞", "-∞", "NaN")),
                "from organic yesterday today select round(0.0), round(-0.0), round(-0.3), round(1.0/0.0), round(-1.0/0.0), round(0.0/0.0)");
    }

    @Test
    public void testRoundDigitsOverLimit() throws Exception {
        QueryServletTestUtils.expectException(
                AllData.DATASET,
                "from organic yesterday today select round(101.46, 11)",
                QueryServletTestUtils.LanguageVersion.IQL2,
                errorMsg -> errorMsg.contains("The max digits for ROUND is 10"));
        QueryServletTestUtils.expectException(
                AllData.DATASET,
                "from organic yesterday today select round(101.46, -11)",
                QueryServletTestUtils.LanguageVersion.IQL2,
                errorMsg -> errorMsg.contains("The max digits for ROUND is 10"));
    }

    @Test
    public void testModulus() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "1", "0", "0", "1"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select 3 % 2, 2 % 2, 100 % 2, count() % 2");
    }

    @Test
    public void testPower() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "9", "27", String.valueOf(1.4142136), String.valueOf(151 * 151), String.valueOf(12.2882057)));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select 3 ^ 2, 3 ^ 3, 2 ^ 0.5, count() ^ 2, count() ^ 0.5");
    }

    @Test
    public void testIfThenElse() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "100", "0"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select if count() > 0 then 100 else 0, if count() <= 0 then 100 else 0");
    }

    @Test
    public void testIfThenElseBothBranches() throws Exception {
        // Testing that both branches of 'if' statement are processed during execution
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4", "0", "0"));
        expected.add(ImmutableList.of("b", "2", "4", "4"));
        expected.add(ImmutableList.of("c", "4", "2", "2"));
        expected.add(ImmutableList.of("d", "141", "4", "4"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected,
                "from organic yesterday today group by tk select count(), lag(1, count()), if (tk=\"a\"+tk=\"c\") > 0 then lag(1, count()) else lag(1, count())");
    }

    @Test
    public void testNamed() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151", "156", String.valueOf(151 * 151)));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select count() as c, c + 5, c * c");
    }

    @Test
    public void testParent() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "151", "4"));
        expected.add(ImmutableList.of("b", "151", "2"));
        expected.add(ImmutableList.of("c", "151", "4"));
        expected.add(ImmutableList.of("d", "151", "141"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected,
                "from organic yesterday today group by tk select parent(count()), count()", true);
        QueryServletTestUtils.testIQL2(AllData.DATASET, QueryServletTestUtils.addConstantColumn(1, "1", expected),
                "from organic yesterday today group by tk, (true) having count() > 0 select parent(parent(count())), count()", true);
    }

    // TODO: LAG(0, X) should work like X.
    @Test
    public void testLag() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "10", "0"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "60", "10"));
        expected.add(ImmutableList.of("[2015-01-01 03:00:00, 2015-01-01 04:00:00)", "1", "60", "60"));
        expected.add(ImmutableList.of("[2015-01-01 04:00:00, 2015-01-01 05:00:00)", "1", "1", "60"));
        expected.add(ImmutableList.of("[2015-01-01 05:00:00, 2015-01-01 06:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 06:00:00, 2015-01-01 07:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 07:00:00, 2015-01-01 08:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 08:00:00, 2015-01-01 09:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 09:00:00, 2015-01-01 10:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 10:00:00, 2015-01-01 11:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 11:00:00, 2015-01-01 12:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 12:00:00, 2015-01-01 13:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 13:00:00, 2015-01-01 14:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 14:00:00, 2015-01-01 15:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 15:00:00, 2015-01-01 16:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 16:00:00, 2015-01-01 17:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 17:00:00, 2015-01-01 18:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 18:00:00, 2015-01-01 19:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 19:00:00, 2015-01-01 20:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 20:00:00, 2015-01-01 21:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 21:00:00, 2015-01-01 22:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 22:00:00, 2015-01-01 23:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "1", "1", "1"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today group by time(1h) select count(), lag(1, count()), lag(2, count())");
    }

    @Test
    public void testIterateLag() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4", "0", "0"));
        expected.add(ImmutableList.of("b", "2", "4", "0"));
        expected.add(ImmutableList.of("c", "4", "2", "4"));
        expected.add(ImmutableList.of("d", "141", "4", "2"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today group by tk select count(), lag(1, count()), lag(2, count())");
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected,
                "from organic yesterday today group by tk in (\"a\", \"b\", \"c\", \"d\") select count(), lag(1, count()), lag(2, count())");
    }

    @Test
    public void testIterateLagInt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("3", "1", "0"));
        expected.add(ImmutableList.of("5", "1", "499"));
        expected.add(ImmutableList.of("7", "1", "699"));
        expected.add(ImmutableList.of("100", "1", "9999"));
        expected.add(ImmutableList.of("1000", "1", "99999"));
        QueryServletTestUtils.testIQL2(
                AllData.DATASET,
                expected,
                "from organic yesterday today group by oji in (3, 5, 7, 100, 1000) select COUNT(), if (lag(1,count()) > 0) then oji * 100 * count() - lag(1, count()) else 0",
                true);
    }

    @Test
    public void testWindow() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "10", "10"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "70", "70"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "120", "130"));
        expected.add(ImmutableList.of("[2015-01-01 03:00:00, 2015-01-01 04:00:00)", "1", "61", "131"));
        expected.add(ImmutableList.of("[2015-01-01 04:00:00, 2015-01-01 05:00:00)", "1", "2", "132"));
        expected.add(ImmutableList.of("[2015-01-01 05:00:00, 2015-01-01 06:00:00)", "1", "2", "123"));
        expected.add(ImmutableList.of("[2015-01-01 06:00:00, 2015-01-01 07:00:00)", "1", "2", "64"));
        expected.add(ImmutableList.of("[2015-01-01 07:00:00, 2015-01-01 08:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 08:00:00, 2015-01-01 09:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 09:00:00, 2015-01-01 10:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 10:00:00, 2015-01-01 11:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 11:00:00, 2015-01-01 12:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 12:00:00, 2015-01-01 13:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 13:00:00, 2015-01-01 14:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 14:00:00, 2015-01-01 15:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 15:00:00, 2015-01-01 16:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 16:00:00, 2015-01-01 17:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 17:00:00, 2015-01-01 18:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 18:00:00, 2015-01-01 19:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 19:00:00, 2015-01-01 20:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 20:00:00, 2015-01-01 21:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 21:00:00, 2015-01-01 22:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 22:00:00, 2015-01-01 23:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "1", "2", "5"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today group by time(1h) select count(), window(2, count()), window(5, count())");
    }

    @Test
    public void testRunning() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "10"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "70"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "130"));
        expected.add(ImmutableList.of("[2015-01-01 03:00:00, 2015-01-01 04:00:00)", "1", "131"));
        expected.add(ImmutableList.of("[2015-01-01 04:00:00, 2015-01-01 05:00:00)", "1", "132"));
        expected.add(ImmutableList.of("[2015-01-01 05:00:00, 2015-01-01 06:00:00)", "1", "133"));
        expected.add(ImmutableList.of("[2015-01-01 06:00:00, 2015-01-01 07:00:00)", "1", "134"));
        expected.add(ImmutableList.of("[2015-01-01 07:00:00, 2015-01-01 08:00:00)", "1", "135"));
        expected.add(ImmutableList.of("[2015-01-01 08:00:00, 2015-01-01 09:00:00)", "1", "136"));
        expected.add(ImmutableList.of("[2015-01-01 09:00:00, 2015-01-01 10:00:00)", "1", "137"));
        expected.add(ImmutableList.of("[2015-01-01 10:00:00, 2015-01-01 11:00:00)", "1", "138"));
        expected.add(ImmutableList.of("[2015-01-01 11:00:00, 2015-01-01 12:00:00)", "1", "139"));
        expected.add(ImmutableList.of("[2015-01-01 12:00:00, 2015-01-01 13:00:00)", "1", "140"));
        expected.add(ImmutableList.of("[2015-01-01 13:00:00, 2015-01-01 14:00:00)", "1", "141"));
        expected.add(ImmutableList.of("[2015-01-01 14:00:00, 2015-01-01 15:00:00)", "1", "142"));
        expected.add(ImmutableList.of("[2015-01-01 15:00:00, 2015-01-01 16:00:00)", "1", "143"));
        expected.add(ImmutableList.of("[2015-01-01 16:00:00, 2015-01-01 17:00:00)", "1", "144"));
        expected.add(ImmutableList.of("[2015-01-01 17:00:00, 2015-01-01 18:00:00)", "1", "145"));
        expected.add(ImmutableList.of("[2015-01-01 18:00:00, 2015-01-01 19:00:00)", "1", "146"));
        expected.add(ImmutableList.of("[2015-01-01 19:00:00, 2015-01-01 20:00:00)", "1", "147"));
        expected.add(ImmutableList.of("[2015-01-01 20:00:00, 2015-01-01 21:00:00)", "1", "148"));
        expected.add(ImmutableList.of("[2015-01-01 21:00:00, 2015-01-01 22:00:00)", "1", "149"));
        expected.add(ImmutableList.of("[2015-01-01 22:00:00, 2015-01-01 23:00:00)", "1", "150"));
        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "1", "151"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today group by time(1h) select count(), running(count())");
    }

    // TODO: Make a real test for percentile calculations
    @Test
    public void testPercentile1() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "1", "1", "1"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select percentile(allbit, 0.00001), percentile(allbit, 50), percentile(allbit, 100)", true);
    }

    @Test
    public void testMedian() throws Exception {
        QueryServletTestUtils.testIQL2(AllData.DATASET, ImmutableList.of(ImmutableList.of("","1")), "from organic yesterday today select median(allbit)", true);
        QueryServletTestUtils.testIQL2(AllData.DATASET, ImmutableList.of(ImmutableList.of("","10")), "from organic 2015-01-01 00:00:00  2015-01-01 01:00:00 select median(oji)", true);
        QueryServletTestUtils.testIQL2(AllData.DATASET, ImmutableList.of(ImmutableList.of("","13")), "from organic 2015-01-01 03:00:00 2015-01-02 00:00:00 select median(oji)", true);
        QueryServletTestUtils.testIQL2(AllData.DATASET, ImmutableList.of(ImmutableList.of("","1")), "from organic 2015-01-01 03:00:00 2015-01-02 00:00:00 select median(ojc)", true);
    }

    @Test
    public void sumAcross() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151", "4", "302"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select sum_over(tk, count()), sum_over(tk, 1), sum_over(tk, [2])");
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "306", "2653", "151")),
                "from organic yesterday today select sum_over(oji, ojc), sum_over(ojc, oji), sum_over(tk, count())", true);
    }

    @Test
    public void multiDatasetSumAcross() throws Exception {
        QueryServletTestUtils.testIQL2(
                AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "306")),
                "from organic yesterday today, distinct select sum_over(organic.oji, organic.ojc)",
                true
        );

        QueryServletTestUtils.testIQL2(
                AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "1180")),
                "from organic yesterday today, distinct select sum_over(organic.tk HAVING count() < 60, organic.oji)",
                true
        );
    }

    @Test
    public void testRegroupIntoParent() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151", "151", "151"));
        // SUM_OVER and RUNNING in one query cause RegroupIntoParent command to happen.
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "FROM organic yesterday today SELECT count(), SUM_OVER(time(1d), COUNT()), RUNNING(COUNT())");
    }

    @Test
    public void testAVG() throws  Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "118", "0.3"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic 2015-01-01 00:00 2015-01-01 01:00 SELECT AVG(oji), AVG(DISTINCT(tk))");
    }

    @Test
    public void testMultiAVG() throws  Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "118", "10", "0.3", "25.43", "25.43"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected,
                "from organic 2015-01-01 00:00 2015-01-01 01:00 as o1, organic 2015-01-01 01:00 2015-01-01 02:00 as o2 " +
                        "SELECT AVG(o1.oji), AVG(o2.oji), AVG(DISTINCT(o1.tk)), PRINTF('%.2f', AVG(oji)), PRINTF('%.2f', AVG(o1.oji+o2.oji))");
    }
}
