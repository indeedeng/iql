package com.indeed.squall.iql2.language;

import com.google.common.base.Function;
import com.indeed.squall.iql2.language.query.Queries;
import junit.framework.Assert;
import org.junit.Test;

public class IdentifiersTest {
    public static final Function<JQLParser, String> PARSE_IDENTIFIER = new Function<JQLParser, String>() {
        @Override
        public String apply(JQLParser input) {
            return Identifiers.parseIdentifier(input.identifier()).unwrap();
        }
    };

    @Test
    public void test() throws Exception {
        Assert.assertEquals("HI", Queries.runParser("hi", PARSE_IDENTIFIER));
        Assert.assertEquals("HI", Queries.runParser("`hi`", PARSE_IDENTIFIER));
        Assert.assertEquals("_HI_", Queries.runParser("`_hi_`", PARSE_IDENTIFIER));
        Assert.assertEquals("ABC123", Queries.runParser("abc123", PARSE_IDENTIFIER));
        Assert.assertEquals("ABC-123", Queries.runParser("`abc-123`", PARSE_IDENTIFIER));
        Assert.assertEquals("YYYYMMDD", Queries.runParser("yyyymmdd", PARSE_IDENTIFIER));
    }
}