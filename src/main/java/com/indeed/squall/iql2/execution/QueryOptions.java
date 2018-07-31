package com.indeed.squall.iql2.execution;

/**
 * @author aibragimov
 *
 * This is list of all supported options with description gathered in one place.
 */
public class QueryOptions {

    // Don't do cache lookup, force run query.
    public static final String NO_CACHE = "nocache";

    // Enable port forwarding to use rust daemon
    public static final String USE_RUST_DAEMON = "rust";

    // Temporary features, now in test mode.
    // After testing should be deleted or moved to main features list.
    public static class Experimental {
        private Experimental() {
        }
    }

    private QueryOptions() {
    }
}
