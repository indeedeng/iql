package com.indeed.jql.language;

import org.antlr.v4.runtime.misc.NotNull;

public class DocMetrics {
    public static DocMetric parseDocMetric(JQLParser.DocMetricContext metricContext) {
        throw new UnsupportedOperationException();
    }

    public static DocMetric parseJqlDocMetric(JQLParser.JqlDocMetricContext metricContext) {
        final DocMetric[] ref = new DocMetric[1];

        metricContext.enterRule(new JQLBaseListener() {
            private void accept(DocMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterDocCounts(@NotNull JQLParser.DocCountsContext ctx) {
                accept(new DocMetric.Field("count()"));
            }

            public void enterDocSignum(@NotNull JQLParser.DocSignumContext ctx) {
                accept(new DocMetric.Signum(parseJqlDocMetric(ctx.jqlDocMetric())));
            }

            public void enterDocMinus(@NotNull JQLParser.DocMinusContext ctx) {
                accept(new DocMetric.Subtract(parseJqlDocMetric(ctx.jqlDocMetric(0)), parseJqlDocMetric(ctx.jqlDocMetric(1))));
            }

            public void enterDocMod(@NotNull JQLParser.DocModContext ctx) {
                accept(new DocMetric.Modulus(parseJqlDocMetric(ctx.jqlDocMetric(0)), parseJqlDocMetric(ctx.jqlDocMetric(1))));
            }

            public void enterDocPlus(@NotNull JQLParser.DocPlusContext ctx) {
                accept(new DocMetric.Add(parseJqlDocMetric(ctx.jqlDocMetric(0)), parseJqlDocMetric(ctx.jqlDocMetric(1))));
            }

            public void enterDocMetricParens(@NotNull JQLParser.DocMetricParensContext ctx) {
                accept(parseJqlDocMetric(ctx.jqlDocMetric()));
            }

            public void enterDocDiv(@NotNull JQLParser.DocDivContext ctx) {
                accept(new DocMetric.Divide(parseJqlDocMetric(ctx.jqlDocMetric(0)), parseJqlDocMetric(ctx.jqlDocMetric(1))));
            }

            public void enterDocAbs(@NotNull JQLParser.DocAbsContext ctx) {
                accept(new DocMetric.Abs(parseJqlDocMetric(ctx.jqlDocMetric())));
            }

            public void enterDocNegate(@NotNull JQLParser.DocNegateContext ctx) {
                accept(new DocMetric.Negate(parseJqlDocMetric(ctx.jqlDocMetric())));
            }

            public void enterDocIfThenElse(@NotNull JQLParser.DocIfThenElseContext ctx) {
                final DocFilter condition = DocFilters.parseJqlDocFilter(ctx.jqlDocFilter());
                final DocMetric trueCase = parseJqlDocMetric(ctx.trueCase);
                final DocMetric falseCase = parseJqlDocMetric(ctx.falseCase);
                accept(new DocMetric.IfThenElse(condition, trueCase, falseCase));
            }

            public void enterDocInt(@NotNull JQLParser.DocIntContext ctx) {
                accept(new DocMetric.Constant(Long.parseLong(ctx.INT().getText())));
            }

            public void enterDocMult(@NotNull JQLParser.DocMultContext ctx) {
                accept(new DocMetric.Multiply(parseJqlDocMetric(ctx.jqlDocMetric(0)), parseJqlDocMetric(ctx.jqlDocMetric(1))));
            }

            @Override
            public void enterDocAtom(@NotNull JQLParser.DocAtomContext ctx) {
                accept(parseDocMetricAtom(ctx.docMetricAtom()));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc metric: [" + metricContext.getText() + "]");
        }
        return ref[0];
    }

    public static DocMetric parseDocMetricAtom(JQLParser.DocMetricAtomContext docMetricAtomContext) {
        final DocMetric[] ref = new DocMetric[1];

        docMetricAtomContext.enterRule(new JQLBaseListener() {
            private void accept(DocMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterDocMetricAtomHasntInt(@NotNull JQLParser.DocMetricAtomHasntIntContext ctx) {
                final String field = ctx.field.getText();
                final long term = Long.parseLong(ctx.INT().getText());
                accept(negateMetric(new DocMetric.HasInt(field, term)));
            }

            public void enterDocMetricAtomHasntString(@NotNull JQLParser.DocMetricAtomHasntStringContext ctx) {
                accept(negateMetric(new DocMetric.HasString(ctx.field.getText(), ctx.term.getText())));
            }

            public void enterDocMetricAtomFloatScale(@NotNull JQLParser.DocMetricAtomFloatScaleContext ctx) {
                final String field = ctx.field.getText();
                final double mult = Double.parseDouble(ctx.mult.getText());
                final double add = Double.parseDouble(ctx.add.getText());
                accept(new DocMetric.FloatScale(field, mult, add));
            }

            public void enterDocMetricAtomHasString(@NotNull JQLParser.DocMetricAtomHasStringContext ctx) {
                accept(new DocMetric.HasString(ctx.field.getText(), ctx.term.getText()));
            }

            public void enterDocMetricAtomHasStringQuoted(@NotNull JQLParser.DocMetricAtomHasStringQuotedContext ctx) {
                final HasTermQuote hasTermQuote = HasTermQuote.create(ctx.STRING_LITERAL().getText());
                accept(new DocMetric.HasString(hasTermQuote.getField(), hasTermQuote.getTerm()));
            }

            public void enterDocMetricAtomHasInt(@NotNull JQLParser.DocMetricAtomHasIntContext ctx) {
                final String field = ctx.field.getText();
                final long term = Long.parseLong(ctx.INT().getText());
                accept(new DocMetric.HasInt(field, term));
            }

            public void enterDocMetricAtomRawField(@NotNull JQLParser.DocMetricAtomRawFieldContext ctx) {
                accept(new DocMetric.Field(ctx.identifier().getText()));
            }

            public void enterDocMetricAtomHasIntQuoted(@NotNull JQLParser.DocMetricAtomHasIntQuotedContext ctx) {
                final HasTermQuote hasTermQuote = HasTermQuote.create(ctx.STRING_LITERAL().getText());
                final long termInt = Long.parseLong(hasTermQuote.getTerm());
                accept(new DocMetric.HasInt(hasTermQuote.getField(), termInt));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc metric: [" + docMetricAtomContext.getText() + "]");
        }
        return ref[0];
    }

    public static DocMetric negateMetric(DocMetric metric) {
        return new DocMetric.Subtract(new DocMetric.Constant(1), metric);
    }

    public static class HasTermQuote {
        private final String field;
        private final String term;

        private HasTermQuote(String field, String term) {
            this.field = field;
            this.term = term;
        }

        public static HasTermQuote create(String s) {
            final String unquoted = ParserCommon.unquote(s);
            final int colon = unquoted.indexOf(':');
            final String field = unquoted.substring(0, colon);
            final String term = unquoted.substring(colon + 1);
            return new HasTermQuote(field, term);
        }

        public String getField() {
            return field;
        }

        public String getTerm() {
            return term;
        }
    }
}
