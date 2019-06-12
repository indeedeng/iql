/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.language;

import com.indeed.iql.exceptions.IqlKnownException;
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
    MONTH(0L, "MMMM yyyy", "month"),
    YEAR(0L, "yyyy", "year"),
    QUARTER(0L, null, "quarter"),
    BUCKETS(0L, null, "b");

    public final long millis;
    public final String formatString;
    public final String identifier;

    TimeUnit(final long millis, final String formatString, final String identifier) {
        this.millis = millis;
        this.formatString = formatString;
        this.identifier = identifier;
    }

    public static TimeUnit fromChar(final char c, final boolean useLegacy) {
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
            case 'q':
            case 'Q':
                return QUARTER;
            default:
                throw new IqlKnownException.ParseErrorException("Invalid time unit: " + c);
        }
    }

    public static TimeUnit fromString(final String s, final boolean useLegacy) {
        if (s.length() == 1) {
            return fromChar(s.charAt(0), useLegacy);
        }
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
        } else if ("quarters".startsWith(lowerTimeUnit)) {
            return QUARTER;
        }
        throw new IqlKnownException.ParseErrorException("Don't know how to turn into TimeUnit: " + s);
    }

    public static DateTime subtract(final DateTime start, final int value, final TimeUnit unit) {
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
            case QUARTER:
                return start.minusMonths(3);
            default:
                throw new IqlKnownException.ParseErrorException("Unknown time unit: " + unit);
        }
    }

    public long toSeconds() {
        return millis/1000;
    }
}
