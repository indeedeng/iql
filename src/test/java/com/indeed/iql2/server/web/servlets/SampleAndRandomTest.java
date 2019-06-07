package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.LanguageVersion;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.Options;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.ResultFormat;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.expectException;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.expectExceptionAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.runQuery;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

/**
 * Golden dataset testing to ensure that we don't CHANGE the results of
 * SAMPLE / RANDOM for a given seed.
 * This is important for a number of use-cases.
 *
 * Yes, all of these tests *are* dependent on RNG, but that RNG needs to stay
 * consistent. So these tests do make sense over time.
 */
public class SampleAndRandomTest extends BasicTest {
    @Test
    public void testSampleCountry() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("au", "1"));
        expected.add(ImmutableList.of("co", "1"));
        expected.add(ImmutableList.of("ec", "1"));
        expected.add(ImmutableList.of("es", "1"));
        expected.add(ImmutableList.of("gb", "1"));
        expected.add(ImmutableList.of("hu", "1"));
        expected.add(ImmutableList.of("il", "1"));
        expected.add(ImmutableList.of("se", "1"));
        expected.add(ImmutableList.of("sg", "1"));
        expected.add(ImmutableList.of("tr", "1"));
        expected.add(ImmutableList.of("ua", "1"));

        testAll(expected, "from countries yesterday today where sample(country, 1, 10, \"foo\") group by country", Options.create(true));
    }

    @Test
    public void testStringSampleConsistency() throws Exception {
        final String query =
                "FROM countries yesterday today as all, countries(SAMPLE(country, 1, 2, \"\")) as sampled\n" +
                "GROUP BY DATASET()\n" +
                "SELECT count(), [m(SAMPLE(country, 1, 2, \"\"))]";
        final ImmutableList<List<String>> expected = ImmutableList.of(
                ImmutableList.of("all", "64", "33"),
                ImmutableList.of("sampled", "33", "33")
        );
        testIQL2(expected, query, Options.create(true));
    }

    @Test
    public void testStringSampleConsistencyUneven() throws Exception {
        final String query =
                "FROM countries yesterday today as all, countries(SAMPLE(country, 8, 10, \"\")) as sampled\n" +
                "GROUP BY DATASET()\n" +
                "SELECT count(), [m(SAMPLE(country, 8, 10, \"\"))]";
        final ImmutableList<List<String>> expected = ImmutableList.of(
                ImmutableList.of("all", "64", "53"),
                ImmutableList.of("sampled", "53", "53")
        );
        testIQL2(expected, query, Options.create(true));
    }

    @Test
    public void testIntSampleConsistency() throws Exception {
        final String query =
                "FROM countries yesterday today as all, countries(SAMPLE(random, 1, 2, \"aoe\")) as sampled\n" +
                "GROUP BY DATASET()\n" +
                "SELECT count(), [m(SAMPLE(random, 1, 2, \"aoe\"))]";
        // The fact that this is also 33 is just a coincidence
        final ImmutableList<List<String>> expected = ImmutableList.of(
                ImmutableList.of("all", "64", "33"),
                ImmutableList.of("sampled", "33", "33") // <-- this number matching 3 / 4 numbers is the key
        );
        testIQL2(expected, query, Options.create(true));
    }

    @Test
    public void testIntSampleConsistencyUneven() throws Exception {
        final String query =
                "FROM countries yesterday today as all, countries(SAMPLE(random, 8, 10, \"aoe\")) as sampled\n" +
                "GROUP BY DATASET()\n" +
                "SELECT count(), [m(SAMPLE(random, 8, 10, \"aoe\"))]";
        // The fact that this is also 33 is just a coincidence
        final ImmutableList<List<String>> expected = ImmutableList.of(
                ImmutableList.of("all", "64", "50"),
                ImmutableList.of("sampled", "50", "50") // <-- this number matching 3 / 4 numbers is the key
        );
        testIQL2(expected, query, Options.create(true));
    }

    @Test
    public void testQuoteInSalt() throws Exception {
        runQuery(
            "from countries yesterday today " +
                    "where sample(country, 50, 100, '\"salt with quotes \"\"\"\"') " +
                    "group by random(country, 10, '\"more quotes\"\\'') " +
                    "select [m(sample(country, 5, 10, '\"quotey\"'))], [random(country, 5, '\"quotey\"')]",
                LanguageVersion.IQL2,
                ResultFormat.EVENT_STREAM,
                Options.create(true),
                Collections.emptySet()
        );
    }

    @Test
    public void testRandomCountry() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "ae", "1"));
        expected.add(ImmutableList.of("4", "aq", "1"));
        expected.add(ImmutableList.of("3", "ar", "1"));
        expected.add(ImmutableList.of("3", "at", "1"));
        expected.add(ImmutableList.of("4", "au", "1"));
        expected.add(ImmutableList.of("2", "be", "1"));
        expected.add(ImmutableList.of("4", "bh", "1"));
        expected.add(ImmutableList.of("4", "br", "1"));
        expected.add(ImmutableList.of("3", "ca", "1"));
        expected.add(ImmutableList.of("3", "ch", "1"));
        expected.add(ImmutableList.of("1", "cl", "1"));
        expected.add(ImmutableList.of("2", "cn", "1"));
        expected.add(ImmutableList.of("3", "co", "1"));
        expected.add(ImmutableList.of("3", "cr", "1"));
        expected.add(ImmutableList.of("4", "cz", "1"));
        expected.add(ImmutableList.of("3", "de", "1"));
        expected.add(ImmutableList.of("2", "dk", "1"));
        expected.add(ImmutableList.of("2", "ec", "1"));
        expected.add(ImmutableList.of("2", "eg", "1"));
        expected.add(ImmutableList.of("3", "es", "1"));
        expected.add(ImmutableList.of("4", "fi", "1"));
        expected.add(ImmutableList.of("1", "fr", "1"));
        expected.add(ImmutableList.of("3", "gb", "1"));
        expected.add(ImmutableList.of("3", "gr", "1"));
        expected.add(ImmutableList.of("1", "hk", "1"));
        expected.add(ImmutableList.of("3", "hu", "1"));
        expected.add(ImmutableList.of("2", "id", "1"));
        expected.add(ImmutableList.of("1", "ie", "1"));
        expected.add(ImmutableList.of("2", "il", "1"));
        expected.add(ImmutableList.of("3", "in", "1"));
        expected.add(ImmutableList.of("4", "it", "1"));
        expected.add(ImmutableList.of("2", "jp", "1"));
        expected.add(ImmutableList.of("1", "kr", "1"));
        expected.add(ImmutableList.of("1", "kw", "1"));
        expected.add(ImmutableList.of("4", "lu", "1"));
        expected.add(ImmutableList.of("3", "ma", "1"));
        expected.add(ImmutableList.of("1", "mx", "1"));
        expected.add(ImmutableList.of("2", "my", "1"));
        expected.add(ImmutableList.of("1", "ng", "1"));
        expected.add(ImmutableList.of("4", "nl", "1"));
        expected.add(ImmutableList.of("2", "no", "1"));
        expected.add(ImmutableList.of("1", "nz", "1"));
        expected.add(ImmutableList.of("1", "om", "1"));
        expected.add(ImmutableList.of("3", "pa", "1"));
        expected.add(ImmutableList.of("3", "pe", "1"));
        expected.add(ImmutableList.of("2", "ph", "1"));
        expected.add(ImmutableList.of("2", "pk", "1"));
        expected.add(ImmutableList.of("4", "pl", "1"));
        expected.add(ImmutableList.of("1", "pt", "1"));
        expected.add(ImmutableList.of("4", "qa", "1"));
        expected.add(ImmutableList.of("4", "ro", "1"));
        expected.add(ImmutableList.of("1", "ru", "1"));
        expected.add(ImmutableList.of("2", "sa", "1"));
        expected.add(ImmutableList.of("3", "se", "1"));
        expected.add(ImmutableList.of("4", "sg", "1"));
        expected.add(ImmutableList.of("3", "th", "1"));
        expected.add(ImmutableList.of("1", "tr", "1"));
        expected.add(ImmutableList.of("1", "tw", "1"));
        expected.add(ImmutableList.of("3", "ua", "1"));
        expected.add(ImmutableList.of("2", "us", "1"));
        expected.add(ImmutableList.of("2", "uy", "1"));
        expected.add(ImmutableList.of("1", "ve", "1"));
        expected.add(ImmutableList.of("4", "vn", "1"));
        expected.add(ImmutableList.of("4", "za", "1"));

        testIQL2(expected, "from countries yesterday today group by random(country, 4, \"seed\"), country", Options.create(true));
    }

    @Test
    public void testRandomStringConsistency() throws Exception {
        final String query =
                "from countries yesterday today " +
                "GROUP BY RANDOM(country, 5, \"salt\") " +
                "SELECT AVG(RANDOM(country, 5, \"salt\"))";

        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("No term", "NaN"));
        expected.add(ImmutableList.of("1", "1"));
        expected.add(ImmutableList.of("2", "2"));
        expected.add(ImmutableList.of("3", "3"));
        expected.add(ImmutableList.of("4", "4"));
        expected.add(ImmutableList.of("5", "5"));

        testIQL2(expected, query, Options.create(true));
    }

    @Test
    public void testRandomIntConsistency() throws Exception {
        final String query =
                "from countries yesterday today " +
                "GROUP BY RANDOM(random, 5, \"salt2\") " +
                "SELECT " +
                        "AVG(RANDOM(random, 5, \"salt2\")), " +
                        "AVG(RANDOM(random + 0, 5, \"salt2\"))";

        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("No term", "NaN", "NaN"));
        expected.add(ImmutableList.of("1", "1", "1"));
        expected.add(ImmutableList.of("2", "2", "2"));
        expected.add(ImmutableList.of("3", "3", "3"));
        expected.add(ImmutableList.of("4", "4", "4"));
        expected.add(ImmutableList.of("5", "5", "5"));

        testIQL2(expected, query, Options.create(true));
    }

    @Test
    public void testSampleParamCheck() throws Exception {
        // check that query fails during verification
        final Predicate<String> wrongSampleParams =
                s -> s.contains("Wrong params for SAMPLE: expected 0 <= numerator <= denominator");

        // doc filter
        expectExceptionAll("from organic yesterday today where sample(oji, 10, 1)", wrongSampleParams);
        // doc metric
        expectException("from organic yesterday today where sample(oji + ojc, 10, 1)", LanguageVersion.IQL2, wrongSampleParams);
        // group by filter
        expectException("from organic yesterday today group by sample(oji, 10, 1)", LanguageVersion.IQL2, wrongSampleParams);
    }

    @Test
    public void testRandomCountryWithLargeBucketNumber() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("413", "ae", "1"));
        expected.add(ImmutableList.of("1812", "aq", "1"));
        expected.add(ImmutableList.of("783", "ar", "1"));
        expected.add(ImmutableList.of("171", "at", "1"));
        expected.add(ImmutableList.of("924", "au", "1"));
        expected.add(ImmutableList.of("1058", "be", "1"));
        expected.add(ImmutableList.of("160", "bh", "1"));
        expected.add(ImmutableList.of("88", "br", "1"));
        expected.add(ImmutableList.of("1183", "ca", "1"));
        expected.add(ImmutableList.of("835", "ch", "1"));
        expected.add(ImmutableList.of("649", "cl", "1"));
        expected.add(ImmutableList.of("1058", "cn", "1"));
        expected.add(ImmutableList.of("411", "co", "1"));
        expected.add(ImmutableList.of("343", "cr", "1"));
        expected.add(ImmutableList.of("392", "cz", "1"));
        expected.add(ImmutableList.of("279", "de", "1"));
        expected.add(ImmutableList.of("1978", "dk", "1"));
        expected.add(ImmutableList.of("206", "ec", "1"));
        expected.add(ImmutableList.of("122", "eg", "1"));
        expected.add(ImmutableList.of("87", "es", "1"));
        expected.add(ImmutableList.of("868", "fi", "1"));
        expected.add(ImmutableList.of("941", "fr", "1"));
        expected.add(ImmutableList.of("47", "gb", "1"));
        expected.add(ImmutableList.of("887", "gr", "1"));
        expected.add(ImmutableList.of("125", "hk", "1"));
        expected.add(ImmutableList.of("855", "hu", "1"));
        expected.add(ImmutableList.of("630", "id", "1"));
        expected.add(ImmutableList.of("1841", "ie", "1"));
        expected.add(ImmutableList.of("1630", "il", "1"));
        expected.add(ImmutableList.of("1335", "in", "1"));
        expected.add(ImmutableList.of("984", "it", "1"));
        expected.add(ImmutableList.of("1418", "jp", "1"));
        expected.add(ImmutableList.of("1765", "kr", "1"));
        expected.add(ImmutableList.of("985", "kw", "1"));
        expected.add(ImmutableList.of("768", "lu", "1"));
        expected.add(ImmutableList.of("579", "ma", "1"));
        expected.add(ImmutableList.of("1697", "mx", "1"));
        expected.add(ImmutableList.of("1422", "my", "1"));
        expected.add(ImmutableList.of("277", "ng", "1"));
        expected.add(ImmutableList.of("220", "nl", "1"));
        expected.add(ImmutableList.of("114", "no", "1"));
        expected.add(ImmutableList.of("1085", "nz", "1"));
        expected.add(ImmutableList.of("629", "om", "1"));
        expected.add(ImmutableList.of("1999", "pa", "1"));
        expected.add(ImmutableList.of("1363", "pe", "1"));
        expected.add(ImmutableList.of("326", "ph", "1"));
        expected.add(ImmutableList.of("734", "pk", "1"));
        expected.add(ImmutableList.of("52", "pl", "1"));
        expected.add(ImmutableList.of("1625", "pt", "1"));
        expected.add(ImmutableList.of("376", "qa", "1"));
        expected.add(ImmutableList.of("1236", "ro", "1"));
        expected.add(ImmutableList.of("1161", "ru", "1"));
        expected.add(ImmutableList.of("94", "sa", "1"));
        expected.add(ImmutableList.of("931", "se", "1"));
        expected.add(ImmutableList.of("1448", "sg", "1"));
        expected.add(ImmutableList.of("1927", "th", "1"));
        expected.add(ImmutableList.of("157", "tr", "1"));
        expected.add(ImmutableList.of("253", "tw", "1"));
        expected.add(ImmutableList.of("351", "ua", "1"));
        expected.add(ImmutableList.of("1502", "us", "1"));
        expected.add(ImmutableList.of("598", "uy", "1"));
        expected.add(ImmutableList.of("1513", "ve", "1"));
        expected.add(ImmutableList.of("1840", "vn", "1"));
        expected.add(ImmutableList.of("1348", "za", "1"));

        testIQL2(expected, "from countries yesterday today group by random(country, 2000, \"seed\"), country", Options.create(true));
    }

    @Test
    public void testRandomLimitRejection() throws Exception {
        runQuery("from countries yesterday today group by random(country, 100000)", LanguageVersion.IQL2, ResultFormat.EVENT_STREAM, Options.create(false), Collections.emptySet());
    }

    @Test
    public void testRandomMetricLimitRejection() throws Exception {
        runQuery("from countries yesterday today group by random(random+5, 100000)", LanguageVersion.IQL2, ResultFormat.EVENT_STREAM, Options.create(false), Collections.emptySet());
    }
}
