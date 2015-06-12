package com.indeed.jql.language;

import com.indeed.util.core.Pair;
import org.antlr.v4.runtime.Token;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;

public class ParserCommon {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    public static List<Pair<Integer, TimeUnit>> parseTimePeriod(JQLParser.TimePeriodContext timePeriodContext) {
        if (timePeriodContext instanceof JQLParser.TimePeriodParseableContext) {
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
            return result;
        } else if (timePeriodContext instanceof JQLParser.TimePeriodStringLiteralContext) {
            final String unquoted = ParserCommon.unquote(((JQLParser.TimePeriodStringLiteralContext) timePeriodContext).STRING_LITERAL().getText());
            final JQLParser parser = Main.parserForString(unquoted);
            final List<Pair<Integer, TimeUnit>> result = parseTimePeriod(parser.timePeriod());
            if (parser.getNumberOfSyntaxErrors() > 0) {
                throw new IllegalArgumentException("Syntax errors encountered parsing quoted time period: [" + unquoted + "]");
            }
            return result;
        } else {
            throw new IllegalArgumentException("Failed to handle time period context: [" + timePeriodContext.getText() + "]");
        }

    }

    public static String unquote(String text) {
        final StringBuilder sb = new StringBuilder();
        boolean isEscaping = false;
        for (int i = 1; i < text.length() - 1; i++) {
            final char c = text.charAt(i);
            if (c == '\\') {
                if (isEscaping) {
                    sb.append('\\');
                    isEscaping = false;
                } else {
                    isEscaping = true;
                }
            } else {
                sb.append(c);
                isEscaping = false;
            }
        }
        return sb.toString();
    }
}
