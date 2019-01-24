package com.indeed.iql2.server.web.servlets.query;

import java.util.Set;

public class ErrorCollector {
    private final Set<String> errors;
    private final Set<String> warnings;

    public ErrorCollector(final Set<String> errors, final Set<String> warnings) {
        this.errors = errors;
        this.warnings = warnings;
    }

    public void error(final String error) {
        errors.add(error);
    }

    public void warn(final String warn) {
        warnings.add(warn);
    }
}
