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
}
