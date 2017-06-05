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

    public static TimeUnit fromChar(char c) {
        switch (c) {
            case 's': return SECOND;
            case 'm': return MINUTE;
            case 'h': return HOUR;
            case 'd': return DAY;
            case 'w': case 'W': return WEEK;
            case 'M': return MONTH;
            case 'b': return BUCKETS;
            case 'y': return YEAR;
            default:
                throw new IllegalArgumentException("Invalid time unit: " + c);
        }
    }

    public static TimeUnit fromString(String s) {
        if (s.length() == 1) {
            return fromChar(s.charAt(0));
        } else {
            if ("seconds".startsWith(s)) {
                return SECOND;
            } else if ("minutes".startsWith(s)) {
                return MINUTE;
            } else if ("hours".startsWith(s)) {
                return HOUR;
            } else if ("days".startsWith(s)) {
                return DAY;
            } else if ("weeks".startsWith(s)) {
                return WEEK;
            } else if ("months".startsWith(s)) {
                return MONTH;
            } else if ("buckets".startsWith(s)) {
                return BUCKETS;
            } else if ("years".startsWith(s)) {
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
