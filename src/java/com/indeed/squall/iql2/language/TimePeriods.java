package com.indeed.squall.iql2.language;

import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.util.core.Pair;
import org.antlr.v4.runtime.Token;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TimePeriods {
    public static List<Pair<Integer, TimeUnit>> parseTimePeriod(JQLParser.TimePeriodContext timePeriodContext) {
        if (timePeriodContext == null) {
            return Collections.singletonList(Pair.of(1, TimeUnit.HOUR));
        } else if (timePeriodContext instanceof JQLParser.TimePeriodParseableContext) {
            final JQLParser.TimePeriodParseableContext periodContext = (JQLParser.TimePeriodParseableContext) timePeriodContext;
            final List<Token> coeffs = periodContext.coeffs;
            final List<Token> units = periodContext.units;
            if (coeffs.size() != units.size()) {
                throw new IllegalArgumentException("How did I get here?");
            }
            final List<Pair<Integer, TimeUnit>> result = new ArrayList<>();
            for (int i = 0; i < coeffs.size(); i++) {
                final int coeff = Integer.parseInt(coeffs.get(i).getText());
                final TimeUnit unit = TimeUnit.fromString(units.get(i).getText());
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

                    final int coeff = Integer.parseInt(raw.substring(numberStart, numberEndExcl));
                    final TimeUnit unit = TimeUnit.fromString(raw.substring(periodStart, periodEndExcl));
                    result.add(Pair.of(coeff, unit));

                    start = current;
                }
            }
            return result;
        } else if (timePeriodContext instanceof JQLParser.TimePeriodStringLiteralContext) {
            final String unquoted = ParserCommon.unquote(((JQLParser.TimePeriodStringLiteralContext) timePeriodContext).STRING_LITERAL().getText());
            final JQLParser parser = Queries.parserForString(unquoted);
            final List<Pair<Integer, TimeUnit>> result = parseTimePeriod(parser.timePeriod());
            if (parser.getNumberOfSyntaxErrors() > 0) {
                throw new IllegalArgumentException("Syntax errors encountered parsing quoted time period: [" + unquoted + "]");
            }
            return result;
        } else {
            throw new IllegalArgumentException("Failed to handle time period context: [" + timePeriodContext.getText() + "]");
        }

    }

    public static DateTime timePeriodDateTime(JQLParser.TimePeriodContext timePeriodContext) {
        final List<Pair<Integer, TimeUnit>> pairs = parseTimePeriod(timePeriodContext);
        DateTime dt = DateTime.now().withTimeAtStartOfDay();
        for (final Pair<Integer, TimeUnit> pair : pairs) {
            dt = TimeUnit.subtract(dt, pair.getFirst(), pair.getSecond());
        }
        return dt;
    }
}
