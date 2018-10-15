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

package com.indeed.iql2.sqltoiql;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Arrays;

public enum IQLOperator {
    EQUALS("="),
    NOT_EQUALS("!="),
    LESS_THAN("<"),
    GREATER_THAN(">");


    private final String value;
    IQLOperator(final String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value;
    }

    public String getValue() {
        return this.value;
    }

    private static ImmutableMap<String, IQLOperator> reverseLookup =
            Maps.uniqueIndex(Arrays.asList(IQLOperator.values()), IQLOperator::getValue);

    public static IQLOperator fromString(final String id) {
        if (!reverseLookup.containsKey(id)) {
            throw new UnknownOperatorException(id);
        }
        return reverseLookup.get(id);
    }

    public static class UnknownOperatorException extends RuntimeException {
        public UnknownOperatorException(final String operator) {
            super("Unknown IQL operator: "+operator);
        }
    }
}
