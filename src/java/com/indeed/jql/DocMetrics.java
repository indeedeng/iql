package com.indeed.jql;

import org.antlr.v4.runtime.misc.NotNull;

public class DocMetrics {
    public static DocMetric parseDocMetric(JQLParser.DocMetricContext metricContext) {
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
                accept(new DocMetric.Signum(parseDocMetric(ctx.docMetric())));
            }

            public void enterDocHasIntQuoted(@NotNull JQLParser.DocHasIntQuotedContext ctx) {
                final HasTermQuote hasTermQuote = new HasTermQuote(ctx.STRING_LITERAL().getText());
                final long termInt = Long.parseLong(hasTermQuote.getTerm());
                accept(new DocMetric.HasInt(hasTermQuote.getField(), termInt));
            }

            public void enterDocMinus(@NotNull JQLParser.DocMinusContext ctx) {
                accept(new DocMetric.Subtract(parseDocMetric(ctx.docMetric(0)), parseDocMetric(ctx.docMetric(1))));
            }

            public void enterDocMod(@NotNull JQLParser.DocModContext ctx) {
                accept(new DocMetric.Modulus(parseDocMetric(ctx.docMetric(0)), parseDocMetric(ctx.docMetric(1))));
            }

            public void enterDocPlus(@NotNull JQLParser.DocPlusContext ctx) {
                accept(new DocMetric.Add(parseDocMetric(ctx.docMetric(0)), parseDocMetric(ctx.docMetric(1))));
            }

            public void enterDocHasStringQuoted(@NotNull JQLParser.DocHasStringQuotedContext ctx) {
                final HasTermQuote hasTermQuote = new HasTermQuote(ctx.STRING_LITERAL().getText()).invoke();
                accept(new DocMetric.HasString(hasTermQuote.getField(), hasTermQuote.getTerm()));
            }

            public void enterDocHasInt(@NotNull JQLParser.DocHasIntContext ctx) {
                final String field = ctx.field.getText();
                final long term = Long.parseLong(ctx.INT().getText());
                accept(new DocMetric.HasInt(field, term));
            }

            public void enterDocRawField(@NotNull JQLParser.DocRawFieldContext ctx) {
                accept(new DocMetric.Field(ctx.identifier().getText()));
            }

            public void enterDocMetricParens(@NotNull JQLParser.DocMetricParensContext ctx) {
                accept(parseDocMetric(ctx.docMetric()));
            }

            public void enterDocFloatScale(@NotNull JQLParser.DocFloatScaleContext ctx) {
                final String field = ctx.field.getText();
                final double mult = Double.parseDouble(ctx.mult.getText());
                final double add = Double.parseDouble(ctx.add.getText());
                accept(new DocMetric.FloatScale(field, mult, add));
            }

            public void enterDocHasString(@NotNull JQLParser.DocHasStringContext ctx) {
                accept(new DocMetric.HasString(ctx.field.getText(), ctx.term.getText()));
            }

            public void enterDocDiv(@NotNull JQLParser.DocDivContext ctx) {
                accept(new DocMetric.Divide(parseDocMetric(ctx.docMetric(0)), parseDocMetric(ctx.docMetric(1))));
            }

            public void enterDocAbs(@NotNull JQLParser.DocAbsContext ctx) {
                accept(new DocMetric.Abs(parseDocMetric(ctx.docMetric())));
            }

            public void enterDocNegate(@NotNull JQLParser.DocNegateContext ctx) {
                accept(new DocMetric.Negate(parseDocMetric(ctx.docMetric())));
            }

            public void enterDocHasntInt(@NotNull JQLParser.DocHasntIntContext ctx) {
                final String field = ctx.field.getText();
                final long term = Long.parseLong(ctx.INT().getText());
                accept(negateMetric(new DocMetric.HasInt(field, term)));
            }

            public void enterDocHasntString(@NotNull JQLParser.DocHasntStringContext ctx) {
                accept(negateMetric(new DocMetric.HasString(ctx.field.getText(), ctx.term.getText())));
            }

            public void enterDocIfThenElse(@NotNull JQLParser.DocIfThenElseContext ctx) {
                final DocFilter condition = DocFilters.parseDocFilter(ctx.docFilter());
                final DocMetric trueCase = parseDocMetric(ctx.trueCase);
                final DocMetric falseCase = parseDocMetric(ctx.falseCase);
                accept(new DocMetric.IfThenElse(condition, trueCase, falseCase));
            }

            public void enterDocInt(@NotNull JQLParser.DocIntContext ctx) {
                accept(new DocMetric.Constant(Long.parseLong(ctx.INT().getText())));
            }

            public void enterDocMult(@NotNull JQLParser.DocMultContext ctx) {
                accept(new DocMetric.Multiply(parseDocMetric(ctx.docMetric(0)), parseDocMetric(ctx.docMetric(1))));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc metric: [" + metricContext.getText() + "]");
        }
        return ref[0];
    }

    public static DocMetric negateMetric(DocMetric metric) {
        return new DocMetric.Subtract(new DocMetric.Constant(1), metric);
    }

    // TODO: Man IDEA's generated methods suck with multiple returns....
    public static class HasTermQuote {
        private final String s;
        private String field;
        private String term;

        public HasTermQuote(String s) {
            this.s = s;
        }

        public String getField() {
            return field;
        }

        public String getTerm() {
            return term;
        }

        public HasTermQuote invoke() {
            final String unquoted = ParserCommon.unquote(s);
            final int colon = unquoted.indexOf(':');
            field = unquoted.substring(0, colon);
            term = unquoted.substring(colon + 1);
            return this;
        }
    }
}
