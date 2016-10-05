package com.indeed.squall.iql2.server.print;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 */
public class PrettyPrintTest {
    @Test
    public void prettyPrint() throws Exception {
        Assert.assertEquals("FROM jobsearch 2d 1d\nSELECT COUNT()", PrettyPrint.prettyPrint("from jobsearch 2d 1d select count()"));
        Assert.assertEquals("FROM jobsearch yesterday today\nSELECT oji", PrettyPrint.prettyPrint("from jobsearch yesterday today select oji"));
        Assert.assertEquals("FROM jobsearch yesterday today\nSELECT (oji + ojc)", PrettyPrint.prettyPrint("from jobsearch yesterday today select oji+ojc"));
        Assert.assertEquals("FROM jobsearch yesterday today\nWHERE (country=\"us\") (oji=10)\nSELECT COUNT()", PrettyPrint.prettyPrint("from jobsearch yesterday today where country:\"us\" oji:10 select count()"));
        Assert.assertEquals("FROM jobsearch yesterday today\nWHERE (ctk=~\".*\")\nSELECT COUNT()", PrettyPrint.prettyPrint("from jobsearch yesterday today where ctk=~\".*\" select count()"));
        Assert.assertEquals("FROM jobsearch yesterday today\nWHERE (escaped=\"stuff\\\"\")\nSELECT COUNT()", PrettyPrint.prettyPrint("from jobsearch yesterday today where escaped='stuff\"' select count()"));
    }

    @Test
    public void stringEscape() throws Exception {
        Assert.assertEquals("abc\\\"def", PrettyPrint.stringEscape("abc\"def"));
        Assert.assertEquals("abc\\ndef", PrettyPrint.stringEscape("abc\ndef"));
    }
}