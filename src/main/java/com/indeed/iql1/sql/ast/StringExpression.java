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

package com.indeed.iql1.sql.ast;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * A string literal.
 *
 * @author Ben Yu
 */
@JsonSerialize
public final class StringExpression extends ValueObject implements Expression {
    private final String string;

    public StringExpression(String string) {
        this.string = string;
    }

    @JsonProperty
    public String getString() {
        return string;
    }

    public <Z> Z match(final Matcher<Z> matcher) {
        return matcher.stringExpression(string);
    }
}
