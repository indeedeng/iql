package com.indeed.iql2.language.query;

import com.indeed.iql2.language.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.Months;
import org.joda.time.Years;

public enum UnevenGroupByPeriod {
    MONTH,
    QUARTER,
    YEAR;

    public static UnevenGroupByPeriod fromTimeUnit(final TimeUnit timeUnit) {
        switch (timeUnit) {
            case MONTH:
                return MONTH;
            case YEAR:
                return YEAR;
            case QUARTER:
                return QUARTER;
            default:
                throw new IllegalArgumentException("Not a valid jagged group by type: " + timeUnit);
        }
    }

    /**
     * Returns the inclusive start boundary of the period that contains the given dateTime.
     */
    public DateTime startOfPeriod(final DateTime dateTime) {
        switch (this) {
            case MONTH:
                return dateTime.withDayOfMonth(1).withTimeAtStartOfDay();
            case QUARTER:
                final int inputMonth = dateTime.getMonthOfYear();
                final int outputMonth = inputMonth - ((inputMonth - 1) % 3);
                return dateTime.withMonthOfYear(outputMonth).withDayOfMonth(1).withTimeAtStartOfDay();
            case YEAR:
                return dateTime.withDayOfYear(1).withTimeAtStartOfDay();
            default:
                throw new IllegalArgumentException("Unexpected value: " + this);
        }
    }

    /**
     * Returns the exclusive end boundary of the period that contains the given dateTime.
     */
    public DateTime endOfPeriod(final DateTime dateTime) {
        switch (this) {
            case MONTH:
                return startOfPeriod(dateTime).plusMonths(1);
            case QUARTER:
                return startOfPeriod(dateTime).plusMonths(3);
            case YEAR:
                return startOfPeriod(dateTime).plusYears(1);
            default:
                throw new IllegalArgumentException("Unexpected value: " + this);
        }
    }

    /**
     * Returns the start time of the period numPeriods away from the given start period.
     * It is assumed that the given start period represents the beginning of a period.
     */
    public DateTime plusPeriods(final DateTime start, final int numPeriods) {
        switch (this) {
            case MONTH:
                return start.plusMonths(numPeriods);
            case QUARTER:
                return start.plusMonths(3 * numPeriods);
            case YEAR:
                return start.plusYears(numPeriods);
            default:
                throw new IllegalArgumentException("Unexpected value: " + this);
        }
    }

    /**
     * Count the number of periods between start (inclusive) and end (exclusive).
     * Both start and end are assumed to be datetimes that represent the boundary
     * of a period according to this GroupByType.
     */
    public int periodsBetween(final DateTime start, final DateTime end) {
        switch (this) {
            case MONTH:
                return Months.monthsBetween(start, end).getMonths();
            case QUARTER:
                return Months.monthsBetween(start, end).getMonths() / 3;
            case YEAR:
                return Years.yearsBetween(start, end).getYears();
            default:
                throw new IllegalArgumentException("Unexpected value: " + this);
        }
    }
}
