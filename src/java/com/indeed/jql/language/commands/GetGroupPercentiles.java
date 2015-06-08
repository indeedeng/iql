package com.indeed.jql.language.commands;

import java.util.Arrays;
import java.util.Set;

public class GetGroupPercentiles implements Command {
    public final Set<String> scope;
    public final String field;
    public final double[] percentiles;

    public GetGroupPercentiles(Set<String> scope, String field, double[] percentiles) {
        this.scope = scope;
        this.field = field;
        this.percentiles = percentiles;
    }

    @Override
    public String toString() {
        return "GetGroupPercentiles{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", percentiles=" + Arrays.toString(percentiles) +
                '}';
    }
}
