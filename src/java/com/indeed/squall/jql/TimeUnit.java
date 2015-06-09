package com.indeed.squall.jql;

import org.joda.time.DateTime;

/**
 * @author jwolfe
 */
public enum TimeUnit {

    SECOND(1000L, "yyyy-MM-dd HH:mm:ss"),
    MINUTE(1000L * 60, "yyyy-MM-dd HH:mm"),
    HOUR(1000L * 60 * 60, "yyyy-MM-dd HH"),
    DAY(1000L * 60 * 60 * 24, "yyyy-MM-dd"),
    WEEK(1000L * 60 * 60 * 24 * 7, "yyyy-MM-dd"),
    MONTH(TimeUnit.DAY.millis, "MMMM yyyy");

    public final long millis;
    public final String formatString;

    TimeUnit(long millis, String formatString) {
        this.millis = millis;
        this.formatString = formatString;
    }

    public static TimeUnit fromChar(char c) {
        switch (c) {
            case 's': return SECOND;
            case 'm': return MINUTE;
            case 'h': return HOUR;
            case 'd': return DAY;
            case 'w': return WEEK;
            case 'M': return MONTH;
            default:
                throw new IllegalArgumentException("Invalid time unit: " + c);
        }
    }

    public static DateTime subtract(DateTime start, int value, TimeUnit unit) {
        switch (unit) {
            case SECOND:
                return start.minusSeconds(value);
            case MINUTE:
                return start.minusMinutes(value);
            case HOUR:
                return start.minusHours(value);
            case DAY:
                return start.minusDays(value);
            case WEEK:
                return start.minusWeeks(value);
            case MONTH:
                return start.minusMonths(value);
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unit);
        }
    }
}
