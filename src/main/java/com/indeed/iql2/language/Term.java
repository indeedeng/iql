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

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Comparator;


/**
 * This class represents a term that was parsed from some string (from original query or from subquery results)
 *
 * It has 3 possible states.
 *
 * 1. String, that is not integer
 *       stringTerm = someValue, intTerm = 0, isIntTerm = false
 *
 * 2. String, that is 'usual' integer. This means that Long.toString(intTerm) gives original string.
 *       stringTerm = null, intTerm = someValue, isIntTerm = true
 *
 * 3. String, that is 'unusual' integer. This means that Long.toString(intTerm) for some reason does not give original string.
 *     For example, leading zeroes in string or '+' sign.
 *       stringTerm = originalString, intTerm = someValue, isIntTerm = true
 *
 *     We need to distinguish 2 and 3 because we need different behavior depending of field type
 *     and what operation with term we want to perform
 */

@EqualsAndHashCode
@ToString
public class Term {
    private final String stringTerm;
    private final long intTerm;
    private final boolean isIntTerm;

    private Term(final String stringTerm, final long intTerm, final boolean isIntTerm) {
        this.stringTerm = stringTerm;
        this.intTerm = intTerm;
        this.isIntTerm = isIntTerm;
    }

    public static Term term(final String term) {
        try {
            final long value = Long.parseLong(term);
            if (Long.toString(value).equals(term)) {
                // 'usual' integer term
                return new Term(null, value, true);
            } else {
                // 'unusual' integer term
                return new Term(term, value, true);
            }
        } catch (final NumberFormatException ignored) {
            // string term
            return new Term(term, 0, false);
        }
    }

    public static Term term(final long term) {
        return new Term(null, term, true);
    }

    public String asString() {
        return (stringTerm != null) ? stringTerm : Long.toString(intTerm);
    }

    public boolean isIntTerm() {
        return isIntTerm;
    }

    public long getIntTerm() {
        return intTerm;
    }

    public boolean isSafeAsInt() {
        return stringTerm == null;
    }

    public static Term parseJqlTerm(final JQLParser.JqlTermValContext jqlTermValContext) {
        final Term[] ref = new Term[1];

        jqlTermValContext.enterRule(new JQLBaseListener() {
            private void accept(final Term value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterJqlIntTerm(final JQLParser.JqlIntTermContext ctx) {
                accept(term(ctx.integer().getText()));
            }

            public void enterJqlStringTerm(final JQLParser.JqlStringTermContext ctx) {
                if (ctx.STRING_LITERAL() != null) {
                    accept(term(ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
                }
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled term value: [" + jqlTermValContext.getText() + "]");
        }

        return ref[0];
    }

    public static Term parseLegacyTerm(final JQLParser.LegacyTermValContext legacyTermValContext) {
        final Term[] ref = new Term[1];

        legacyTermValContext.enterRule(new JQLBaseListener() {
            private void accept(final Term value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterLegacyIntTerm(final JQLParser.LegacyIntTermContext ctx) {
                accept(term(ctx.integer().getText()));
            }

            public void enterLegacyStringTerm(final JQLParser.LegacyStringTermContext ctx) {
                if (ctx.STRING_LITERAL() != null) {
                    accept(term(ParserCommon.unquoteLegacy(ctx.STRING_LITERAL().getText())));
                } else if (ctx.identifier() != null) {
                    accept(term(ctx.identifier().getText()));
                }
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled term value: [" + legacyTermValContext.getText() + "]");
        }

        return ref[0];
    }

    public static Term parseTerm(final JQLParser.TermValContext termValContext) {
        if (termValContext.jqlTermVal() != null) {
            return parseJqlTerm(termValContext.jqlTermVal());
        } else if (termValContext.legacyTermVal() != null) {
            return parseLegacyTerm(termValContext.legacyTermVal());
        } else {
            throw new IllegalArgumentException();
        }
    }

    public com.indeed.flamdex.query.Term toFlamdex(final String field, final boolean makeIntTerm) {
        if (makeIntTerm && !isIntTerm) {
            throw new IllegalStateException("Should not be here. Check term type before call.");
        }
        return new com.indeed.flamdex.query.Term(field, makeIntTerm, intTerm, makeIntTerm ? "" : asString());
    }

    public com.indeed.iql2.execution.Term toExecutionTerm() {
        return new com.indeed.iql2.execution.Term(isIntTerm, stringTerm, intTerm);
    }

    // This comparator will sort terms in kinda strange order:
    // negative numbers, all strings, positive numbers.
    // It's used only in PrettyPrint, so it's not a big issue.
    public static Comparator<Term> COMPARATOR = new Comparator<Term>() {
        @Override
        public int compare(final Term term1, final Term term2) {
            final int r = Long.compare(term1.intTerm, term2.intTerm);
            if (r != 0) {
                return r;
            }
            return term1.asString().compareTo(term2.asString());
        }
    };
}
