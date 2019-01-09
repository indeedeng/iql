package com.indeed.iql2;

/**
 * @author michihiko
 */
public class MathUtils {
    /**
     * @return the double value converted to a long, or Long.MAX_VALUE if not possible.
     * (Note: Long.MAX_VALUE cannot be represented as a double, so is a safe sentinel)
     */
    public static long integralDoubleAsLong(final double value) {
        final long longValue = (long) value;
        final double doubleFromLong = (double) longValue;
        if (value == doubleFromLong) {
            return longValue;
        } else {
            return Long.MAX_VALUE;
        }
    }
}
