package com.indeed.squall.iql2.execution;

/**
 * @author aibragimov
 *
 * This is list of all experimental features with description gathered in one place.
 */
public class ExperimentalFeatures {

    // Enable special ftgs processing for simple queries.
    // Simple means one ImhotepSession, no presence indexes and stat indexes are the same as in session's FTGSIterator
    public static final String SIMPLE_PROSESSING = "useSimpleFtgsProcessing";

    // While simple processing use unsorted ftgs if possible.
    // This feature make sence only with SIMPLE_PROSESSING
    public static final String UNSORTED_FTGS = "useUnsortedFtgs";

    private ExperimentalFeatures() {
    }
}
