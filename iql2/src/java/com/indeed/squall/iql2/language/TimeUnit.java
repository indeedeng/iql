package com.indeed.squall.iql2.language;

import org.joda.time.DateTime;

/**
 * @author jwolfe
 */
public enum TimeUnit {

    SECOND(1000L, "yyyy-MM-dd HH:mm:ss", "second"),
    MINUTE(1000L * 60, "yyyy-MM-dd HH:mm", "minute"),
    HOUR(1000L * 60 * 60, "yyyy-MM-dd HH", "hour"),
    DAY(1000L * 60 * 60 * 24, "yyyy-MM-dd", "day"),
    WEEK(1000L * 60 * 60 * 24 * 7, "yyyy-MM-dd", "week"),
    MONTH(TimeUnit.DAY.millis, "MMMM yyyy", "month"),
    YEAR(0L, "yyyy", "year"),
    BUCKETS(0L, null, "b");

    public final long millis;
    public final String formatString;
    public final String identifier;

    TimeUnit(long millis, String formatString, String identifier) {
        this.millis = millis;
        this.formatString = formatString;
        this.identifier = identifier;
    }

    public static TimeUnit fromChar(char c, boolean useLegacy) {
        switch (c) {
            case 's':
            case 'S':
                return SECOND;
            case 'm':
                return MINUTE;
            case 'h':
            case 'H':
                return HOUR;
            case 'd':
            case 'D':
                return DAY;
            case 'w':
            case 'W':
                return WEEK;
            case 'M':
                return useLegacy ? MINUTE : MONTH;
            case 'b':
            case 'B':
                return BUCKETS;
            case 'y':
            case 'Y':
                return YEAR;
            default:
                throw new IllegalArgumentException("Invalid time unit: " + c);
        }
    }

    public static TimeUnit fromString(String s, boolean useLegacy) {
        if (s.length() == 1) {
            return fromChar(s.charAt(0), useLegacy);
        } else {
            final String lowerTimeUnit = s.toLowerCase();
            if ("seconds".startsWith(lowerTimeUnit)) {
                return SECOND;
            } else if ("minutes".startsWith(lowerTimeUnit)) {
                return MINUTE;
            } else if ("hours".startsWith(lowerTimeUnit)) {
                return HOUR;
            } else if ("days".startsWith(lowerTimeUnit)) {
                return DAY;
            } else if ("weeks".startsWith(lowerTimeUnit)) {
                return WEEK;
            } else if ("months".startsWith(lowerTimeUnit)) {
                return MONTH;
            } else if ("buckets".startsWith(lowerTimeUnit)) {
                return BUCKETS;
            } else if ("years".startsWith(lowerTimeUnit)) {
                return YEAR;
            }
        }
        throw new IllegalArgumentException("Don't know how to turn into TimeUnit: " + s);
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
            case YEAR:
                return start.minusYears(value);
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unit);
        }
    }
}
