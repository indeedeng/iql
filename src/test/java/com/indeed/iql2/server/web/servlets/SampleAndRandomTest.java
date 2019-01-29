package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.*;

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
        expected.add(ImmutableList.of("2", "aq", "1"));
        expected.add(ImmutableList.of("4", "ar", "1"));
        expected.add(ImmutableList.of("1", "at", "1"));
        expected.add(ImmutableList.of("2", "au", "1"));
        expected.add(ImmutableList.of("1", "be", "1"));
        expected.add(ImmutableList.of("2", "bh", "1"));
        expected.add(ImmutableList.of("2", "br", "1"));
        expected.add(ImmutableList.of("1", "ca", "1"));
        expected.add(ImmutableList.of("3", "ch", "1"));
        expected.add(ImmutableList.of("3", "cl", "1"));
        expected.add(ImmutableList.of("3", "cn", "1"));
        expected.add(ImmutableList.of("2", "co", "1"));
        expected.add(ImmutableList.of("1", "cr", "1"));
        expected.add(ImmutableList.of("2", "cz", "1"));
        expected.add(ImmutableList.of("4", "de", "1"));
        expected.add(ImmutableList.of("1", "dk", "1"));
        expected.add(ImmutableList.of("3", "ec", "1"));
        expected.add(ImmutableList.of("2", "eg", "1"));
        expected.add(ImmutableList.of("1", "es", "1"));
        expected.add(ImmutableList.of("3", "fi", "1"));
        expected.add(ImmutableList.of("4", "fr", "1"));
        expected.add(ImmutableList.of("3", "gb", "1"));
        expected.add(ImmutableList.of("1", "gr", "1"));
        expected.add(ImmutableList.of("1", "hk", "1"));
        expected.add(ImmutableList.of("4", "hu", "1"));
        expected.add(ImmutableList.of("1", "id", "1"));
        expected.add(ImmutableList.of("2", "ie", "1"));
        expected.add(ImmutableList.of("1", "il", "1"));
        expected.add(ImmutableList.of("3", "in", "1"));
        expected.add(ImmutableList.of("1", "it", "1"));
        expected.add(ImmutableList.of("1", "jp", "1"));
        expected.add(ImmutableList.of("2", "kr", "1"));
        expected.add(ImmutableList.of("1", "kw", "1"));
        expected.add(ImmutableList.of("1", "lu", "1"));
        expected.add(ImmutableList.of("4", "ma", "1"));
        expected.add(ImmutableList.of("4", "mx", "1"));
        expected.add(ImmutableList.of("1", "my", "1"));
        expected.add(ImmutableList.of("2", "ng", "1"));
        expected.add(ImmutableList.of("2", "nl", "1"));
        expected.add(ImmutableList.of("1", "no", "1"));
        expected.add(ImmutableList.of("1", "nz", "1"));
        expected.add(ImmutableList.of("2", "om", "1"));
        expected.add(ImmutableList.of("1", "pa", "1"));
        expected.add(ImmutableList.of("4", "pe", "1"));
        expected.add(ImmutableList.of("3", "ph", "1"));
        expected.add(ImmutableList.of("3", "pk", "1"));
        expected.add(ImmutableList.of("4", "pl", "1"));
        expected.add(ImmutableList.of("3", "pt", "1"));
        expected.add(ImmutableList.of("4", "qa", "1"));
        expected.add(ImmutableList.of("1", "ro", "1"));
        expected.add(ImmutableList.of("4", "ru", "1"));
        expected.add(ImmutableList.of("3", "sa", "1"));
        expected.add(ImmutableList.of("1", "se", "1"));
        expected.add(ImmutableList.of("3", "sg", "1"));
        expected.add(ImmutableList.of("2", "th", "1"));
        expected.add(ImmutableList.of("2", "tr", "1"));
        expected.add(ImmutableList.of("1", "tw", "1"));
        expected.add(ImmutableList.of("2", "ua", "1"));
        expected.add(ImmutableList.of("2", "us", "1"));
        expected.add(ImmutableList.of("4", "uy", "1"));
        expected.add(ImmutableList.of("2", "ve", "1"));
        expected.add(ImmutableList.of("2", "vn", "1"));
        expected.add(ImmutableList.of("1", "za", "1"));

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
    public void testSampleRandomConsistency() throws Exception {
        final String query =
                "FROM countries yesterday today as all, countries(SAMPLE(country, 1, 2, \"salt2\")) as sampled " +
                "GROUP BY DATASET(), RANDOM(country, 2, \"salt2\") " +
                "SELECT count(), [m(SAMPLE(country, 1, 2, \"salt2\"))], AVG(RANDOM(country, 2, \"salt2\"))";
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("all", "No term", "0", "0", "NaN"));
        expected.add(ImmutableList.of("all", "1", "33", "0", "1"));
        expected.add(ImmutableList.of("all", "2", "31", "31", "2"));
        expected.add(ImmutableList.of("sampled", "No term", "0", "0", "NaN"));
        expected.add(ImmutableList.of("sampled", "1", "0", "0", "NaN"));
        expected.add(ImmutableList.of("sampled", "2", "31", "31", "2"));

        testIQL2(expected, query, Options.create(true));
    }

    @Test
    public void testRandomCountryWithLargeBucketNumber() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("15", "ae", "1"));
        expected.add(ImmutableList.of("574", "aq", "1"));
        expected.add(ImmutableList.of("1781", "ar", "1"));
        expected.add(ImmutableList.of("265", "at", "1"));
        expected.add(ImmutableList.of("589", "au", "1"));
        expected.add(ImmutableList.of("75", "be", "1"));
        expected.add(ImmutableList.of("696", "bh", "1"));
        expected.add(ImmutableList.of("561", "br", "1"));
        expected.add(ImmutableList.of("159", "ca", "1"));
        expected.add(ImmutableList.of("1046", "ch", "1"));
        expected.add(ImmutableList.of("1498", "cl", "1"));
        expected.add(ImmutableList.of("1194", "cn", "1"));
        expected.add(ImmutableList.of("934", "co", "1"));
        expected.add(ImmutableList.of("486", "cr", "1"));
        expected.add(ImmutableList.of("606", "cz", "1"));
        expected.add(ImmutableList.of("1952", "de", "1"));
        expected.add(ImmutableList.of("450", "dk", "1"));
        expected.add(ImmutableList.of("1236", "ec", "1"));
        expected.add(ImmutableList.of("686", "eg", "1"));
        expected.add(ImmutableList.of("137", "es", "1"));
        expected.add(ImmutableList.of("1203", "fi", "1"));
        expected.add(ImmutableList.of("1718", "fr", "1"));
        expected.add(ImmutableList.of("1390", "gb", "1"));
        expected.add(ImmutableList.of("32", "gr", "1"));
        expected.add(ImmutableList.of("338", "hk", "1"));
        expected.add(ImmutableList.of("1808", "hu", "1"));
        expected.add(ImmutableList.of("435", "id", "1"));
        expected.add(ImmutableList.of("547", "ie", "1"));
        expected.add(ImmutableList.of("399", "il", "1"));
        expected.add(ImmutableList.of("1093", "in", "1"));
        expected.add(ImmutableList.of("98", "it", "1"));
        expected.add(ImmutableList.of("172", "jp", "1"));
        expected.add(ImmutableList.of("614", "kr", "1"));
        expected.add(ImmutableList.of("209", "kw", "1"));
        expected.add(ImmutableList.of("330", "lu", "1"));
        expected.add(ImmutableList.of("1765", "ma", "1"));
        expected.add(ImmutableList.of("1548", "mx", "1"));
        expected.add(ImmutableList.of("112", "my", "1"));
        expected.add(ImmutableList.of("861", "ng", "1"));
        expected.add(ImmutableList.of("800", "nl", "1"));
        expected.add(ImmutableList.of("186", "no", "1"));
        expected.add(ImmutableList.of("13", "nz", "1"));
        expected.add(ImmutableList.of("904", "om", "1"));
        expected.add(ImmutableList.of("348", "pa", "1"));
        expected.add(ImmutableList.of("1724", "pe", "1"));
        expected.add(ImmutableList.of("1112", "ph", "1"));
        expected.add(ImmutableList.of("1320", "pk", "1"));
        expected.add(ImmutableList.of("1938", "pl", "1"));
        expected.add(ImmutableList.of("1054", "pt", "1"));
        expected.add(ImmutableList.of("1596", "qa", "1"));
        expected.add(ImmutableList.of("197", "ro", "1"));
        expected.add(ImmutableList.of("1751", "ru", "1"));
        expected.add(ImmutableList.of("1169", "sa", "1"));
        expected.add(ImmutableList.of("117", "se", "1"));
        expected.add(ImmutableList.of("1118", "sg", "1"));
        expected.add(ImmutableList.of("512", "th", "1"));
        expected.add(ImmutableList.of("966", "tr", "1"));
        expected.add(ImmutableList.of("105", "tw", "1"));
        expected.add(ImmutableList.of("846", "ua", "1"));
        expected.add(ImmutableList.of("627", "us", "1"));
        expected.add(ImmutableList.of("1596", "uy", "1"));
        expected.add(ImmutableList.of("577", "ve", "1"));
        expected.add(ImmutableList.of("855", "vn", "1"));
        expected.add(ImmutableList.of("156", "za", "1"));

        testIQL2(expected, "from countries yesterday today group by random(country, 2000, \"seed\"), country", Options.create(true));
    }
}
