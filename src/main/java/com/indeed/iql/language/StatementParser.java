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

package com.indeed.iql.language;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StatementParser {
    private static final Pattern selectPattern = Pattern.compile("\\s*(?:select|from)\\s+.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern explainPattern = Pattern.compile("\\s*explain\\s+(.*)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern showPattern = Pattern.compile("\\s*show\\s+(?:tables|datasets).*", Pattern.CASE_INSENSITIVE);
    private static final Pattern describePattern = Pattern.compile("\\s*(?:describe|desc)\\s+(\\w+)(?:(?:\\s+|\\.)(\\w+))?.*", Pattern.CASE_INSENSITIVE);

    private StatementParser() {
    }

    public static IQLStatement parseIQLToStatement(String iql) {
        if(selectPattern.matcher(iql).matches()) {
            return new SelectStatement(iql);
        }

        final Matcher describeMatcher = describePattern.matcher(iql);
        if(describeMatcher.matches()) {
            return new DescribeStatement(describeMatcher.group(1), describeMatcher.group(2));
        }

        if(showPattern.matcher(iql).matches()) {
            return new ShowStatement();
        }

        final Matcher explainMatcher = explainPattern.matcher(iql);
        if(explainMatcher.matches()) {
            return new ExplainStatement(explainMatcher.group(1));
        }

        return InvalidStatement.INSTANCE;
    }
}
