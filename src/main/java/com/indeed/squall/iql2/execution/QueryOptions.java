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
        // Enable special ftgs processing for simple queries.
        // Simple means one ImhotepSession, no presence indexes and stat indexes are the same as in session's FTGSIterator
        public static final String SIMPLE_PROSESSING = "useSimpleFtgsProcessing";

        // While simple processing use unsorted ftgs if possible.
        // This feature make sence only with SIMPLE_PROSESSING
        public static final String UNSORTED_FTGS = "useUnsortedFtgs";

        private Experimental() {
        }
    }

    private QueryOptions() {
    }
}
