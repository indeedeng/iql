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

package com.indeed.iql2.execution.groupkeys;

import com.indeed.iql2.Formatter;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.joda.time.format.DateTimeFormatter;

import java.util.Objects;

@Data
@EqualsAndHashCode(callSuper = false)
public class StringGroupKey extends GroupKey {
    public final String term;

    private StringGroupKey(String term) {
        this.term = term;
    }

    public static StringGroupKey fromTimeRange(final DateTimeFormatter formatter, final long start, final long end, final Formatter escaper) {
        return new StringGroupKey(escaper.escape("[" + formatter.print(start) + ", " + formatter.print(end) + ")"));
    }

    public static StringGroupKey fromPreEscaped(final String term) {
        return new StringGroupKey(term);
    }

    public static StringGroupKey fromTerm(final String term, final Formatter formatter) {
        return new StringGroupKey(formatter.escape(term));
    }

    @Override
    public String render() {
        return term;
    }

    @Override
    public boolean isDefault() {
        return false;
    }
}
