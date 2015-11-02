package com.indeed.squall.iql2.language;

import com.indeed.squall.iql2.language.query.Queries;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class DocMetrics {
    public static DocMetric parseDocMetric(JQLParser.DocMetricContext metricContext, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields) {
        if (metricContext.jqlDocMetric() != null) {
            return parseJQLDocMetric(metricContext.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields);
        }
        if (metricContext.legacyDocMetric() != null) {
            return parseLegacyDocMetric(metricContext.legacyDocMetric(), datasetToKeywordAnalyzerFields);
        }
        throw new UnsupportedOperationException("What do?!");
    }

    public static DocMetric parseLegacyDocMetric(JQLParser.LegacyDocMetricContext legacyDocMetricContext, final Map<String, Set<String>> datasetToKeywordAnalyzerFields) {
        final DocMetric[] ref = new DocMetric[1];

        legacyDocMetricContext.enterRule(new JQLBaseListener() {
            private void accept(DocMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterLegacyDocCounts(JQLParser.LegacyDocCountsContext ctx) {
                accept(new DocMetric.Count());
            }

            public void enterLegacyDocSignum(JQLParser.LegacyDocSignumContext ctx) {
                accept(new DocMetric.Signum(parseLegacyDocMetric(ctx.legacyDocMetric(), datasetToKeywordAnalyzerFields)));
            }

            public void enterLegacyDocMinus(JQLParser.LegacyDocMinusContext ctx) {
                accept(new DocMetric.Subtract(parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields), parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields)));
            }

            public void enterLegacyDocMod(JQLParser.LegacyDocModContext ctx) {
                accept(new DocMetric.Modulus(parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields), parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields)));
            }

            public void enterLegacyDocPlus(JQLParser.LegacyDocPlusContext ctx) {
                accept(new DocMetric.Add(parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields), parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields)));
            }

            public void enterLegacyDocMetricParens(JQLParser.LegacyDocMetricParensContext ctx) {
                accept(parseLegacyDocMetric(ctx.legacyDocMetric(), datasetToKeywordAnalyzerFields));
            }

            public void enterLegacyDocDiv(JQLParser.LegacyDocDivContext ctx) {
                accept(new DocMetric.Divide(parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields), parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields)));
            }

            public void enterLegacyDocAbs(JQLParser.LegacyDocAbsContext ctx) {
                accept(new DocMetric.Abs(parseLegacyDocMetric(ctx.legacyDocMetric(), datasetToKeywordAnalyzerFields)));
            }

            public void enterLegacyDocNegate(JQLParser.LegacyDocNegateContext ctx) {
                accept(new DocMetric.Negate(parseLegacyDocMetric(ctx.legacyDocMetric(), datasetToKeywordAnalyzerFields)));
            }

            public void enterLegacyDocInt(JQLParser.LegacyDocIntContext ctx) {
                accept(new DocMetric.Constant(Long.parseLong(ctx.INT().getText())));
            }

            public void enterLegacyDocMult(JQLParser.LegacyDocMultContext ctx) {
                accept(new DocMetric.Multiply(parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields), parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields)));
            }

            @Override
            public void enterLegacyDocEQ(JQLParser.LegacyDocEQContext ctx) {
                accept(new DocMetric.MetricEqual(
                        parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields),
                        parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields)
                ));
            }

            @Override
            public void enterLegacyDocNEQ(JQLParser.LegacyDocNEQContext ctx) {
                accept(new DocMetric.MetricNotEqual(
                        parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields),
                        parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields)
                ));
            }

            @Override
            public void enterLegacyDocGT(JQLParser.LegacyDocGTContext ctx) {
                accept(new DocMetric.MetricGt(
                        parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields),
                        parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields)
                ));
            }

            @Override
            public void enterLegacyDocGTE(JQLParser.LegacyDocGTEContext ctx) {
                accept(new DocMetric.MetricGte(
                        parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields),
                        parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields)
                ));
            }

            @Override
            public void enterLegacyDocLT(JQLParser.LegacyDocLTContext ctx) {
                accept(new DocMetric.MetricLt(
                        parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields),
                        parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields)
                ));
            }

            @Override
            public void enterLegacyDocLTE(JQLParser.LegacyDocLTEContext ctx) {
                accept(new DocMetric.MetricLte(
                        parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields),
                        parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields)
                ));
            }

            public void enterLegacyDocAtom(JQLParser.LegacyDocAtomContext ctx) {
                accept(parseLegacyDocMetricAtom(ctx.legacyDocMetricAtom()));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc metric: [" + legacyDocMetricContext.getText() + "]");
        }
        return ref[0];
    }

    public static DocMetric parseLegacyDocMetricAtom(JQLParser.LegacyDocMetricAtomContext legacyDocMetricAtomContext) {
        final DocMetric[] ref = new DocMetric[1];

        legacyDocMetricAtomContext.enterRule(new JQLBaseListener() {
            private void accept(DocMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            @Override
            public void enterLegacyDocMetricAtomHasString(JQLParser.LegacyDocMetricAtomHasStringContext ctx) {
                accept(new DocMetric.HasString(ctx.field.getText().toUpperCase(), ParserCommon.unquote(ctx.term.getText())));
            }

            @Override
            public void enterLegacyDocMetricAtomHasntString(JQLParser.LegacyDocMetricAtomHasntStringContext ctx) {
                accept(negateMetric(new DocMetric.HasString(ctx.field.getText().toUpperCase(), ParserCommon.unquote(ctx.term.getText()))));
            }

            @Override
            public void enterLegacyDocMetricAtomHasInt(JQLParser.LegacyDocMetricAtomHasIntContext ctx) {
                final String field = ctx.field.getText().toUpperCase();
                final long term = Long.parseLong(ctx.INT().getText());
                accept(new DocMetric.HasInt(field, term));
            }

            @Override
            public void enterLegacyDocMetricAtomHasntInt(JQLParser.LegacyDocMetricAtomHasntIntContext ctx) {
                final String field = ctx.field.getText().toUpperCase();
                final long term = Long.parseLong(ctx.INT().getText());
                accept(negateMetric(new DocMetric.HasInt(field, term)));
            }

            @Override
            public void enterLegacyDocMetricAtomHasStringQuoted(JQLParser.LegacyDocMetricAtomHasStringQuotedContext ctx) {
                final HasTermQuote hasTermQuote = HasTermQuote.create(ctx.STRING_LITERAL().getText());
                accept(new DocMetric.HasString(hasTermQuote.getField().toUpperCase(), hasTermQuote.getTerm()));
            }

            @Override
            public void enterLegacyDocMetricAtomHasIntQuoted(JQLParser.LegacyDocMetricAtomHasIntQuotedContext ctx) {
                final HasTermQuote hasTermQuote = HasTermQuote.create(ctx.STRING_LITERAL().getText());
                final long termInt = Long.parseLong(hasTermQuote.getTerm());
                accept(new DocMetric.HasInt(hasTermQuote.getField().toUpperCase(), termInt));
            }

            @Override
            public void enterLegacyDocMetricAtomFloatScale(JQLParser.LegacyDocMetricAtomFloatScaleContext ctx) {
                final String field = ctx.field.getText().toUpperCase();
                final double mult = Double.parseDouble(ctx.mult.getText());
                final double add = Double.parseDouble(ctx.add.getText());
                accept(new DocMetric.FloatScale(field, mult, add));
            }

            @Override
            public void enterLegacyDocMetricAtomRawField(JQLParser.LegacyDocMetricAtomRawFieldContext ctx) {
                accept(new DocMetric.Field(ctx.identifier().getText().toUpperCase()));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled legacy doc metric atom: [" + legacyDocMetricAtomContext.getText() + "]");
        }
        return ref[0];
    }

    // .. this used to be more substantial. TODO: Inline this grammar rule?
    public static DocMetric parseJQLSyntacticallyAtomicDocMetricAtom(JQLParser.JqlSyntacticallyAtomicDocMetricAtomContext jqlSyntacticallyAtomicDocMetricAtomContext) {
        final DocMetric[] ref = new DocMetric[1];

        jqlSyntacticallyAtomicDocMetricAtomContext.enterRule(new JQLBaseListener() {
            private void accept(DocMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            @Override
            public void enterDocMetricAtomRawField(JQLParser.DocMetricAtomRawFieldContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                accept(scopedField.wrap(new DocMetric.Field(scopedField.field)));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled jql syntactically atomic doc metric: [" + jqlSyntacticallyAtomicDocMetricAtomContext.getText() + "]");
        }
        return ref[0];
    }

    public static DocMetric parseJQLDocMetric(JQLParser.JqlDocMetricContext metricContext, final Map<String, Set<String>> datasetToKeywordAnalyzerFields, final Map<String, Set<String>> datasetToIntFields) {
        final DocMetric[] ref = new DocMetric[1];

        metricContext.enterRule(new JQLBaseListener() {
            private void accept(DocMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterDocCounts(JQLParser.DocCountsContext ctx) {
                accept(new DocMetric.Count());
            }

            public void enterDocSignum(JQLParser.DocSignumContext ctx) {
                accept(new DocMetric.Signum(parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterDocMinus(JQLParser.DocMinusContext ctx) {
                accept(new DocMetric.Subtract(parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields), parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterDocMod(JQLParser.DocModContext ctx) {
                accept(new DocMetric.Modulus(parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields), parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterDocPlus(JQLParser.DocPlusContext ctx) {
                accept(new DocMetric.Add(parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields), parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterDocMetricParens(JQLParser.DocMetricParensContext ctx) {
                accept(parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields));
            }

            public void enterDocDiv(JQLParser.DocDivContext ctx) {
                accept(new DocMetric.Divide(parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields), parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterDocAbs(JQLParser.DocAbsContext ctx) {
                accept(new DocMetric.Abs(parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterDocNegate(JQLParser.DocNegateContext ctx) {
                accept(new DocMetric.Negate(parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterDocIfThenElse(JQLParser.DocIfThenElseContext ctx) {
                final DocFilter condition = DocFilters.parseJQLDocFilter(ctx.jqlDocFilter(), datasetToKeywordAnalyzerFields, datasetToIntFields, null);
                final DocMetric trueCase = parseJQLDocMetric(ctx.trueCase, datasetToKeywordAnalyzerFields, datasetToIntFields);
                final DocMetric falseCase = parseJQLDocMetric(ctx.falseCase, datasetToKeywordAnalyzerFields, datasetToIntFields);
                accept(new DocMetric.IfThenElse(condition, trueCase, falseCase));
            }

            public void enterDocInt(JQLParser.DocIntContext ctx) {
                accept(new DocMetric.Constant(Long.parseLong(ctx.INT().getText())));
            }

            public void enterDocMult(JQLParser.DocMultContext ctx) {
                accept(new DocMetric.Multiply(parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields), parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            @Override
            public void enterDocEQ(JQLParser.DocEQContext ctx) {
                accept(new DocMetric.MetricEqual(
                        parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields),
                        parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)
                ));
            }

            @Override
            public void enterDocNEQ(JQLParser.DocNEQContext ctx) {
                accept(new DocMetric.MetricNotEqual(
                        parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields),
                        parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)
                ));
            }

            @Override
            public void enterDocGT(JQLParser.DocGTContext ctx) {
                accept(new DocMetric.MetricGt(
                        parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields),
                        parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)
                ));
            }

            @Override
            public void enterDocGTE(JQLParser.DocGTEContext ctx) {
                accept(new DocMetric.MetricGte(
                        parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields),
                        parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)
                ));
            }

            @Override
            public void enterDocLT(JQLParser.DocLTContext ctx) {
                accept(new DocMetric.MetricLt(
                        parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields),
                        parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)
                ));
            }

            @Override
            public void enterDocLTE(JQLParser.DocLTEContext ctx) {
                accept(new DocMetric.MetricLte(
                        parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields),
                        parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)
                ));
            }

            public void enterDocAtom(JQLParser.DocAtomContext ctx) {
                accept(parseJQLDocMetricAtom(ctx.jqlDocMetricAtom()));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc metric: [" + metricContext.getText() + "]");
        }
        return ref[0];
    }

    public static DocMetric parseJQLDocMetricAtom(JQLParser.JqlDocMetricAtomContext jqlDocMetricAtomContext) {
        final DocMetric[] ref = new DocMetric[1];

        jqlDocMetricAtomContext.enterRule(new JQLBaseListener() {
            private void accept(DocMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            @Override
            public void enterDocMetricAtomHasntInt(JQLParser.DocMetricAtomHasntIntContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                final long term = Long.parseLong(ctx.INT().getText());
                accept(scopedField.wrap(negateMetric(new DocMetric.HasInt(scopedField.field, term))));
            }

            @Override
            public void enterDocMetricAtomHasntString(JQLParser.DocMetricAtomHasntStringContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                accept(scopedField.wrap(negateMetric(new DocMetric.HasString(scopedField.field, ParserCommon.unquote(ctx.term.getText())))));
            }

            @Override
            public void enterDocMetricAtomHasString(JQLParser.DocMetricAtomHasStringContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                accept(scopedField.wrap(new DocMetric.HasString(scopedField.field, ParserCommon.unquote(ctx.term.getText()))));
            }

            @Override
            public void enterDocMetricAtomHasInt(JQLParser.DocMetricAtomHasIntContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                final long term = Long.parseLong(ctx.INT().getText());
                accept(scopedField.wrap(new DocMetric.HasInt(scopedField.field, term)));
            }

            @Override
            public void enterDocMetricAtomFloatScale(JQLParser.DocMetricAtomFloatScaleContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                final double mult = Double.parseDouble(ctx.mult.getText());
                final double add = Double.parseDouble(ctx.add.getText());
                accept(scopedField.wrap(new DocMetric.FloatScale(scopedField.field, mult, add)));
            }

            @Override
            public void enterDocMetricAtomHasIntField(JQLParser.DocMetricAtomHasIntFieldContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                accept(scopedField.wrap(new DocMetric.HasIntField(scopedField.field)));
            }

            @Override
            public void enterDocMetricAtomHasStringField(JQLParser.DocMetricAtomHasStringFieldContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                accept(scopedField.wrap(new DocMetric.HasStringField(scopedField.field)));
            }

            @Override
            public void enterSyntacticallyAtomicDocMetricAtom(JQLParser.SyntacticallyAtomicDocMetricAtomContext ctx) {
                accept(parseJQLSyntacticallyAtomicDocMetricAtom(ctx.jqlSyntacticallyAtomicDocMetricAtom()));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled jql doc metric atom: [" + jqlDocMetricAtomContext.getText() + "]");
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
            final String field = unquoted.substring(0, colon).toUpperCase();
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
