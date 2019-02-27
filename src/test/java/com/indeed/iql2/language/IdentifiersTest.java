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

package com.indeed.iql2.language;

import com.indeed.iql2.language.query.Queries;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class IdentifiersTest {
    private static final Function<JQLParser, String> PARSE_IDENTIFIER = new Function<JQLParser, String>() {
        @Override
        public String apply(JQLParser input) {
            return Identifiers.parseIdentifier(input.identifier()).unwrap();
        }
    };

    @Test
    public void test() throws Exception {
        Assert.assertEquals("hi", Queries.runParser("hi", PARSE_IDENTIFIER));
        Assert.assertEquals("hi", Queries.runParser("`hi`", PARSE_IDENTIFIER));
        Assert.assertEquals("_hi_", Queries.runParser("`_hi_`", PARSE_IDENTIFIER));
        Assert.assertEquals("_HI_", Queries.runParser("`_HI_`", PARSE_IDENTIFIER));
        Assert.assertEquals("abc123", Queries.runParser("abc123", PARSE_IDENTIFIER));
        Assert.assertEquals("abc-123", Queries.runParser("`abc-123`", PARSE_IDENTIFIER));
        Assert.assertEquals("yyyymmdd", Queries.runParser("yyyymmdd", PARSE_IDENTIFIER));
    }
}