package com.indeed.jql.language;

import com.indeed.util.core.Pair;
import org.antlr.v4.runtime.Token;

import java.util.ArrayList;
import java.util.List;

public class ParserCommon {
    public static List<Pair<Integer, TimeUnit>> parseTimePeriod(JQLParser.TimePeriodContext timePeriodContext) {
        final List<Token> coeffs = timePeriodContext.coeffs;
        final List<Token> units = timePeriodContext.units;
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
