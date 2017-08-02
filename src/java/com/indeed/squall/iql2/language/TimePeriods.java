package com.indeed.squall.iql2.language;

import com.indeed.common.util.time.WallClock;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.util.core.Pair;
import org.antlr.v4.runtime.Token;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TimePeriods {
    public static List<Pair<Integer, TimeUnit>> parseTimePeriod(JQLParser.TimePeriodContext timePeriodContext, boolean useLegacy) {
        if (timePeriodContext == null) {
            return Collections.singletonList(Pair.of(1, TimeUnit.HOUR));
        } else if (timePeriodContext instanceof JQLParser.TimePeriodParseableContext) {
            final JQLParser.TimePeriodParseableContext periodContext = (JQLParser.TimePeriodParseableContext) timePeriodContext;
            final List<Pair<Integer, TimeUnit>> result = new ArrayList<>();
            for (JQLParser.TimeUnitContext timeunit : periodContext.timeunits) {
                final int coeff = (timeunit.coeff == null) ? 1 : Integer.parseInt(timeunit.coeff.getText());
                final TimeUnit unit = TimeUnit.fromString(timeunit.unit.getText(), useLegacy);
                result.add(Pair.of(coeff, unit));
            }
            for (final Token atom : periodContext.atoms) {
                int start = 0;
                final String raw = atom.getText();
                while (start < raw.length()) {
                    final int numberStart = start;
                    int current = start;
                    while (Character.isDigit(raw.charAt(current))) {
                        current++;
                    }
                    final int numberEndExcl = current;

                    final int periodStart = current;
                    while (current < raw.length() && Character.isAlphabetic(raw.charAt(current))) {
                        current++;
                    }
                    final int periodEndExcl = current;

                    final String coeffString = raw.substring(numberStart, numberEndExcl);
                    final int coeff = coeffString.isEmpty() ? 1 : Integer.parseInt(coeffString);
                    final TimeUnit unit = TimeUnit.fromString(raw.substring(periodStart, periodEndExcl), useLegacy);
                    result.add(Pair.of(coeff, unit));

                    start = current;
                }
            }
            return result;
        } else if (timePeriodContext instanceof JQLParser.TimePeriodStringLiteralContext) {
            final String unquoted = ParserCommon.unquote(((JQLParser.TimePeriodStringLiteralContext) timePeriodContext).STRING_LITERAL().getText());
            final JQLParser parser = Queries.parserForString(unquoted);
            final List<Pair<Integer, TimeUnit>> result = parseTimePeriod(parser.timePeriod(), useLegacy);
            if (parser.getNumberOfSyntaxErrors() > 0) {
                throw new IllegalArgumentException("Syntax errors encountered parsing quoted time period: [" + unquoted + "]");
            }
            return result;
        } else {
            throw new IllegalArgumentException("Failed to handle time period context: [" + timePeriodContext.getText() + "]");
        }

    }

    public static DateTime timePeriodDateTime(JQLParser.TimePeriodContext timePeriodContext, WallClock clock, boolean useLegacy) {
        final List<Pair<Integer, TimeUnit>> pairs = parseTimePeriod(timePeriodContext, useLegacy);
        DateTime dt = new DateTime(clock.currentTimeMillis()).withTimeAtStartOfDay();
        for (final Pair<Integer, TimeUnit> pair : pairs) {
            dt = TimeUnit.subtract(dt, pair.getFirst(), pair.getSecond());
        }
        return dt;
    }
}
