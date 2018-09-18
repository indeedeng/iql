package com.indeed.iql.SQLToIQL;


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
