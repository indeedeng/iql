package com.indeed.iql2.language;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class ParserCommonTest {
    @Test
    public void getUnnecessaryEscapes() {
        Assert.assertEquals(
                Sets.newHashSet('a', 'c', 'd'),
                ParserCommon.getUnnecessaryEscapes("\"\\a\\c\\d\"")
        );
        Assert.assertEquals(
                Collections.emptySet(),
                ParserCommon.getUnnecessaryEscapes("\"this is a test. \\\\ \\r \\t \\n \\u1234 \"")
        );
        Assert.assertEquals(
                Collections.singleton('\''),
                ParserCommon.getUnnecessaryEscapes("\" \\' \"")
        );
        Assert.assertEquals(
                Collections.emptySet(),
                ParserCommon.getUnnecessaryEscapes("\' \\' \'")
        );
        Assert.assertEquals(
                Collections.singleton('\''),
                ParserCommon.getUnnecessaryEscapes("\" \\\' \"")
        );
        Assert.assertEquals(
                Collections.emptySet(),
                ParserCommon.getUnnecessaryEscapes("\" \\\" \"")
        );
    }

    @Test
    public void getUnnecessaryRegexEscapes() {
        Assert.assertEquals(
                Collections.emptySet(),
                ParserCommon.getUnnecessaryRegexEscapes("This is a test with no errors\\. \\| \\? \\* \\+ \\{ \\} \\[ \\] \\^ \\. \\\" \\( \\) \\\\")
        );
        Assert.assertEquals(
                Sets.newHashSet('s', 'w'),
                ParserCommon.getUnnecessaryRegexEscapes("\\w+\\s")
        );
        Assert.assertEquals(
                Sets.newHashSet('d'),
                ParserCommon.getUnnecessaryRegexEscapes("\\d+")
        );
        Assert.assertEquals(
                Collections.emptySet(),
                ParserCommon.getUnnecessaryRegexEscapes("[0-9]+")
        );
    }
}