package com.indeed.imhotep.sql.parser;

import com.google.common.base.Strings;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author vladimir
 */

public class PeriodParser {
    /**
     * Returns a JodaTime Period object representing the provided string period value.
     * Only first symbol of each field tag is mandatory, the rest of the tag is optional. e.g. 1d = 1 day
     * Spacing between the numbers and tags is optional. e.g. 1d = 1 d
     * Having a tag with no number implies quantity of 1. e.g. d = 1d
     * 'ago' suffix is optional.
     * Commas can optionally separate the period parts.
     */
    @Nullable
    public static Period parseString(String value) {
        final String cleanedValue = Strings.nullToEmpty(value).toLowerCase();
        Matcher matcher = relativeDatePattern.matcher(cleanedValue);
        if(!matcher.matches()) {
            return null;
        }
        int years = getValueFromMatch(matcher, 1);
        int months = getValueFromMatch(matcher, 2);
        int weeks = getValueFromMatch(matcher, 3);
        int days = getValueFromMatch(matcher, 4);
        int hours = getValueFromMatch(matcher, 5);
        int minutes = getValueFromMatch(matcher, 6);
        int seconds = getValueFromMatch(matcher, 7);
        return new Period(years, months, weeks, days, hours, minutes, seconds, 0);
    }

    private static final Pattern relativeDatePattern = Pattern.compile(
            "(\\s*(\\d+)?\\s*y(?:ear)?s?\\s*,?\\s*)?" +
            "(\\s*(\\d+)?\\s*mo(?:nth)?s?\\s*,?\\s*)?" +
            "(\\s*(\\d+)?\\s*w(?:eek)?s?\\s*,?\\s*)?" +
            "(\\s*(\\d+)?\\s*d(?:ay)?s?\\s*,?\\s*)?" +
            "(\\s*(\\d+)?\\s*h(?:our)?s?\\s*,?\\s*)?" +
            "(\\s*(\\d+)?\\s*m(?:inute)?s?\\s*,?\\s*)?" +
            "(\\s*(\\d+)?\\s*s(?:econd)?s?\\s*)?" +
            "(?:ago)?\\s*"
    );

    private static int getValueFromMatch(Matcher matcher, int i) {
        final String fieldMatch = matcher.group((i * 2) - 1);
        if(Strings.isNullOrEmpty(fieldMatch)) {
            return 0;
        }
        String value = matcher.group(i * 2);
        return tryParseInt(value, 1);   // if we have a field match then we treat it as 1 by default
    }

    private static int tryParseInt(String val, int def) {
        int retVal;
        try {
            retVal = Integer.parseInt(val);
        } catch (NumberFormatException nfe) {
            retVal = def;
        }
        return retVal;
    }
}
