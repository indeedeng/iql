package com.indeed.iql2.server.web.servlets;

import org.junit.Test;

import java.util.Collections;

public class StringEscapeWarningTest extends BasicTest {
    @Test
    public void testNoIssues() throws Exception {
        QueryServletTestUtils.testWarning(
                Collections.emptyList(),
                "from organic 2d 1d where country=\"bar\\n\\r\\\\\\\"\"",
                QueryServletTestUtils.LanguageVersion.IQL2
        );
    }

    @Test
    public void testSomeCharacters() throws Exception {
        QueryServletTestUtils.testWarning(
                Collections.singletonList("String contains unnecessary escapes (for characters {\'}): \"\\\'bar\""),
                "from organic 2d 1d where country=\"\\\'bar\"",
                QueryServletTestUtils.LanguageVersion.IQL2
        );
        QueryServletTestUtils.testWarning(
                Collections.singletonList("String contains unnecessary escapes (for characters {a}): \"\\a\""),
                "from organic 2d 1d where country=\"\\a\"",
                QueryServletTestUtils.LanguageVersion.IQL2
        );
        QueryServletTestUtils.testWarning(
                Collections.singletonList("String contains unnecessary escapes (for characters {w}): \"\\w\""),
                "from organic 2d 1d where country=\"\\w\"",
                QueryServletTestUtils.LanguageVersion.IQL2
        );
    }
}
