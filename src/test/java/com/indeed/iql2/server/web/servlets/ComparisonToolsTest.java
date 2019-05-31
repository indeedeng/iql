package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.iql2.ComparisonTools;
import org.junit.Test;

public class ComparisonToolsTest {
    private static void testWarning(final String query, final ComparisonTools.Result warning) throws Exception {
        QueryServletTestUtils.testWarning(
                ImmutableList.of("Compatibility warning: " + warning.message),
                query,
                QueryServletTestUtils.LanguageVersion.ORIGINAL_IQL1
        );
    }
    @Test
    public void testWarnings() throws Exception {
        //NotSupportedInLegacy
        testWarning( "from organic 1d 0d where (oji/2) < 5", ComparisonTools.Result.NotSupportedInLegacy);

        //ParsingErrorInLegacy
        testWarning("from organic 2 d 1 d", ComparisonTools.Result.ParsingErrorInLegacy);

        //UnquotedTerm
        testWarning("from organic 1d 0d where tk in (bad term)", ComparisonTools.Result.UnquotedTerm);

        //CommentDiff
        testWarning("from organic 1d 0d -- group by oji", ComparisonTools.Result.CommentDiff);

        //GroupByHour
        testWarning("from organic 1d 0d group by time(1)", ComparisonTools.Result.GroupByHour);

        //TermInParens
        testWarning("from organic 1d 0d where tk = ('a')", ComparisonTools.Result.TermInParens);

        //Unknown
        testWarning("from organic 1d 0d group by time(1d, 'yyyymmdd', unixtime)", ComparisonTools.Result.Unknown);
    }
}
