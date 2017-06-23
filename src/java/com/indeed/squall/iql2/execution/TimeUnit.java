package com.indeed.squall.iql2.execution;

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
}
