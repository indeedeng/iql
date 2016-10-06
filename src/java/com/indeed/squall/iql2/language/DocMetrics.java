package com.indeed.squall.iql2.language;

import com.google.common.collect.Lists;
import com.indeed.common.util.time.WallClock;
import com.indeed.squall.iql2.language.compat.Consumer;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.indeed.squall.iql2.language.Identifiers.parseIdentifier;

public class DocMetrics {
    public static DocMetric parseDocMetric(JQLParser.DocMetricContext metricContext, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields, Consumer<String> warn, WallClock clock) {
        if (metricContext.jqlDocMetric() != null) {
            return parseJQLDocMetric(metricContext.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
        }
        if (metricContext.legacyDocMetric() != null) {
            return parseLegacyDocMetric(metricContext.legacyDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields);
        }
        throw new UnsupportedOperationException("What do?!");
    }

    public static DocMetric parseLegacyDocMetric(JQLParser.LegacyDocMetricContext legacyDocMetricContext, final Map<String, Set<String>> datasetToKeywordAnalyzerFields, final Map<String, Set<String>> datasetToIntFields) {
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
                accept(new DocMetric.Signum(parseLegacyDocMetric(ctx.legacyDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            @Override
            public void enterLegacyDocPlusOrMinus(JQLParser.LegacyDocPlusOrMinusContext ctx) {
                final DocMetric left = parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields);
                final DocMetric right = parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields);
                if (ctx.plus != null) {
                    accept(new DocMetric.Add(left, right));
                } else if (ctx.minus != null) {
                    accept(new DocMetric.Subtract(left, right));
                }
            }

            @Override
            public void enterLegacyDocMultOrDivideOrModulus(JQLParser.LegacyDocMultOrDivideOrModulusContext ctx) {
                final DocMetric left = parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields);
                final DocMetric right = parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields);
                if (ctx.multiply != null) {
                    accept(new DocMetric.Multiply(left, right));
                } else if (ctx.divide != null) {
                    accept(new DocMetric.Divide(left, right));
                } else if (ctx.modulus != null) {
                    accept(new DocMetric.Modulus(left, right));
                }
            }

            public void enterLegacyDocMetricParens(JQLParser.LegacyDocMetricParensContext ctx) {
                accept(parseLegacyDocMetric(ctx.legacyDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields));
            }

            public void enterLegacyDocAbs(JQLParser.LegacyDocAbsContext ctx) {
                accept(new DocMetric.Abs(parseLegacyDocMetric(ctx.legacyDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterLegacyDocNegate(JQLParser.LegacyDocNegateContext ctx) {
                accept(new DocMetric.Negate(parseLegacyDocMetric(ctx.legacyDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterLegacyDocInt(JQLParser.LegacyDocIntContext ctx) {
                accept(new DocMetric.Constant(Long.parseLong(ctx.integer().getText())));
            }

            @Override
            public void enterLegacyDocInequality(JQLParser.LegacyDocInequalityContext ctx) {
                final DocMetric left = parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields);
                final DocMetric right = parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields);
                if (ctx.gte != null) {
                    accept(new DocMetric.MetricGte(left, right));
                } else if (ctx.gt != null) {
                    accept(new DocMetric.MetricGt(left, right));
                } else if (ctx.lte != null) {
                    accept(new DocMetric.MetricLte(left, right));
                } else if (ctx.lt != null) {
                    accept(new DocMetric.MetricLt(left, right));
                } else if (ctx.eq != null) {
                    accept(new DocMetric.MetricEqual(left, right));
                } else if (ctx.neq != null) {
                    accept(new DocMetric.MetricNotEqual(left, right));
                }
            }

            @Override
            public void enterLegacyDocLog(JQLParser.LegacyDocLogContext ctx) {
                final int scaleFactor = ctx.integer() == null ? 1 : Integer.parseInt(ctx.integer().getText());
                accept(new DocMetric.Log(parseLegacyDocMetric(ctx.legacyDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields), scaleFactor));
            }

            @Override
            public void enterLegacyDocExp(JQLParser.LegacyDocExpContext ctx) {
                final int scaleFactor = ctx.integer() == null ? 1 : Integer.parseInt(ctx.integer().getText());
                accept(new DocMetric.Exponentiate(parseLegacyDocMetric(ctx.legacyDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields), scaleFactor));
            }

            @Override
            public void enterLegacyDocMin(JQLParser.LegacyDocMinContext ctx) {
                accept(new DocMetric.Min(parseLegacyDocMetric(ctx.arg1, datasetToKeywordAnalyzerFields, datasetToIntFields), parseLegacyDocMetric(ctx.arg2, datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            @Override
            public void enterLegacyDocMax(JQLParser.LegacyDocMaxContext ctx) {
                accept(new DocMetric.Max(parseLegacyDocMetric(ctx.arg1, datasetToKeywordAnalyzerFields, datasetToIntFields), parseLegacyDocMetric(ctx.arg2, datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterLegacyDocAtom(JQLParser.LegacyDocAtomContext ctx) {
                accept(parseLegacyDocMetricAtom(ctx.legacyDocMetricAtom(), datasetToKeywordAnalyzerFields, datasetToIntFields));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc metric: [" + legacyDocMetricContext.getText() + "]");
        }

        ref[0].copyPosition(legacyDocMetricContext);

        return ref[0];
    }

    public static DocMetric parseLegacyDocMetricAtom(
            JQLParser.LegacyDocMetricAtomContext legacyDocMetricAtomContext,
            final Map<String, Set<String>> datasetToKeywordAnalyzerFields,
            final Map<String, Set<String>> datasetToIntFields
    ) {
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
                accept(new DocMetric.HasString(parseIdentifier(ctx.field), ParserCommon.unquote(ctx.term.getText())));
            }

            @Override
            public void enterLegacyDocMetricAtomHasntString(JQLParser.LegacyDocMetricAtomHasntStringContext ctx) {
                accept(negateMetric(new DocMetric.HasString(parseIdentifier(ctx.field), ParserCommon.unquote(ctx.term.getText()))));
            }

            @Override
            public void enterLegacyDocMetricAtomHasInt(JQLParser.LegacyDocMetricAtomHasIntContext ctx) {
                final Positioned<String> field = parseIdentifier(ctx.field);
                final long term = Long.parseLong(ctx.integer().getText());
                accept(new DocMetric.HasInt(field, term));
            }

            @Override
            public void enterLegacyDocMetricAtomHasntInt(JQLParser.LegacyDocMetricAtomHasntIntContext ctx) {
                final Positioned<String> field = parseIdentifier(ctx.field);
                final long term = Long.parseLong(ctx.integer().getText());
                accept(negateMetric(new DocMetric.HasInt(field, term)));
            }

            @Override
            public void enterLegacyDocMetricAtomHasStringQuoted(JQLParser.LegacyDocMetricAtomHasStringQuotedContext ctx) {
                final HasTermQuote hasTermQuote = HasTermQuote.create(ctx.STRING_LITERAL().getText());
                accept(new DocMetric.HasString(Positioned.unpositioned(hasTermQuote.getField().toUpperCase()), hasTermQuote.getTerm()));
            }

            @Override
            public void enterLegacyDocMetricAtomHasIntQuoted(JQLParser.LegacyDocMetricAtomHasIntQuotedContext ctx) {
                final HasTermQuote hasTermQuote = HasTermQuote.create(ctx.STRING_LITERAL().getText());
                final long termInt = Long.parseLong(hasTermQuote.getTerm());
                accept(new DocMetric.HasInt(Positioned.unpositioned(hasTermQuote.getField().toUpperCase()), termInt));
            }

            @Override
            public void enterLegacyDocMetricAtomFloatScale(JQLParser.LegacyDocMetricAtomFloatScaleContext ctx) {
                final Positioned<String> field = parseIdentifier(ctx.field);
                final double mult = ctx.mult == null ? 1.0 : Double.parseDouble(ctx.mult.getText());
                final double add = ctx.add == null ? 0.0 : Double.parseDouble(ctx.add.getText());
                accept(new DocMetric.FloatScale(field, mult, add));
            }

            @Override
            public void enterLegacyDocMetricAtomRawField(JQLParser.LegacyDocMetricAtomRawFieldContext ctx) {
                accept(new DocMetric.Field(parseIdentifier(ctx.identifier())));
            }

            @Override
            public void enterLegacyDocMetricAtomLucene(final JQLParser.LegacyDocMetricAtomLuceneContext ctx) {
                accept(new DocMetric.Lucene(ParserCommon.unquote(ctx.queryField.getText()), datasetToKeywordAnalyzerFields, datasetToIntFields));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled legacy doc metric atom: [" + legacyDocMetricAtomContext.getText() + "]");
        }

        ref[0].copyPosition(legacyDocMetricAtomContext);

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

        ref[0].copyPosition(jqlSyntacticallyAtomicDocMetricAtomContext);

        return ref[0];
    }

    public static DocMetric parseJQLDocMetric(JQLParser.JqlDocMetricContext metricContext, final Map<String, Set<String>> datasetToKeywordAnalyzerFields, final Map<String, Set<String>> datasetToIntFields, final Consumer<String> warn, final WallClock clock) {
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
                accept(new DocMetric.Signum(parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }

            @Override
            public void enterDocPlusOrMinus(JQLParser.DocPlusOrMinusContext ctx) {
                final DocMetric left = parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                final DocMetric right = parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                if (ctx.plus != null) {
                    accept(new DocMetric.Add(left, right));
                } else if (ctx.minus != null) {
                    accept(new DocMetric.Subtract(left, right));
                }
            }

            @Override
            public void enterDocMultOrDivideOrModulus(JQLParser.DocMultOrDivideOrModulusContext ctx) {
                final DocMetric left = parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                final DocMetric right = parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                if (ctx.multiply != null) {
                    accept(new DocMetric.Multiply(left, right));
                } else if (ctx.divide != null) {
                    accept(new DocMetric.Divide(left, right));
                } else if (ctx.modulus != null) {
                    accept(new DocMetric.Modulus(left, right));
                }
            }

            @Override
            public void enterDocInequality(JQLParser.DocInequalityContext ctx) {
                final DocMetric left = parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                final DocMetric right = parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                if (ctx.gte != null) {
                    accept(new DocMetric.MetricGte(left, right));
                } else if (ctx.gt != null) {
                    accept(new DocMetric.MetricGt(left, right));
                } else if (ctx.lte != null) {
                    accept(new DocMetric.MetricLte(left, right));
                } else if (ctx.lt != null) {
                    accept(new DocMetric.MetricLt(left, right));
                } else if (ctx.eq != null) {
                    accept(new DocMetric.MetricEqual(left, right));
                } else if (ctx.neq != null) {
                    accept(new DocMetric.MetricNotEqual(left, right));
                }
            }

            public void enterDocMetricParens(JQLParser.DocMetricParensContext ctx) {
                accept(parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
            }

            public void enterDocAbs(JQLParser.DocAbsContext ctx) {
                accept(new DocMetric.Abs(parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }

            public void enterDocNegate(JQLParser.DocNegateContext ctx) {
                accept(new DocMetric.Negate(parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }

            public void enterDocIfThenElse(JQLParser.DocIfThenElseContext ctx) {
                final DocFilter condition = DocFilters.parseJQLDocFilter(ctx.jqlDocFilter(), datasetToKeywordAnalyzerFields, datasetToIntFields, null, warn, clock);
                final DocMetric trueCase = parseJQLDocMetric(ctx.trueCase, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                final DocMetric falseCase = parseJQLDocMetric(ctx.falseCase, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                accept(new DocMetric.IfThenElse(condition, trueCase, falseCase));
            }

            public void enterDocInt(JQLParser.DocIntContext ctx) {
                accept(new DocMetric.Constant(Long.parseLong(ctx.integer().getText())));
            }

            @Override
            public void enterDocLog(JQLParser.DocLogContext ctx) {
                final int scaleFactor = ctx.integer() == null ? 1 : Integer.parseInt(ctx.integer().getText());
                accept(new DocMetric.Log(parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock), scaleFactor));
            }

            @Override
            public void enterDocExp(JQLParser.DocExpContext ctx) {
                final int scaleFactor = ctx.integer() == null ? 1 : Integer.parseInt(ctx.integer().getText());
                accept(new DocMetric.Exponentiate(parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock), scaleFactor));
            }

            @Override
            public void enterDocMin(JQLParser.DocMinContext ctx) {
                DocMetric resultMetric = parseJQLDocMetric(ctx.metrics.get(0), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                List<JQLParser.JqlDocMetricContext> metrics = ctx.metrics;
                for (int i = 1; i < metrics.size(); i++) {
                    resultMetric = new DocMetric.Min(resultMetric, parseJQLDocMetric(metrics.get(i), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
                }
                accept(resultMetric);
            }

            @Override
            public void enterDocMax(JQLParser.DocMaxContext ctx) {
                DocMetric resultMetric = parseJQLDocMetric(ctx.metrics.get(0), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                List<JQLParser.JqlDocMetricContext> metrics = ctx.metrics;
                for (int i = 1; i < metrics.size(); i++) {
                    resultMetric = new DocMetric.Max(resultMetric, parseJQLDocMetric(metrics.get(i), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
                }
                accept(resultMetric);
            }

            @Override
            public void enterDocMetricAtomLucene(final JQLParser.DocMetricAtomLuceneContext ctx) {
                accept(new DocMetric.Lucene(ParserCommon.unquote(ctx.queryField.getText()), datasetToKeywordAnalyzerFields, datasetToIntFields));
            }

            public void enterDocAtom(JQLParser.DocAtomContext ctx) {
                accept(parseJQLDocMetricAtom(ctx.jqlDocMetricAtom(), datasetToKeywordAnalyzerFields, datasetToIntFields));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc metric: [" + metricContext.getText() + "]");
        }

        ref[0].copyPosition(metricContext);

        return ref[0];
    }

    public static DocMetric parseJQLDocMetricAtom(
            JQLParser.JqlDocMetricAtomContext jqlDocMetricAtomContext,
            final Map<String, Set<String>> datasetToKeywordAnalyzerFields,
            final Map<String, Set<String>> datasetToIntFields)
    {
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
                final long term = Long.parseLong(ctx.integer().getText());
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
                final long term = Long.parseLong(ctx.integer().getText());
                accept(scopedField.wrap(new DocMetric.HasInt(scopedField.field, term)));
            }

            @Override
            public void enterDocMetricAtomFloatScale(JQLParser.DocMetricAtomFloatScaleContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                final double mult = ctx.mult == null ? 1.0 : Double.parseDouble(ctx.mult.getText());
                final double add = ctx.add == null ? 0.0 : Double.parseDouble(ctx.add.getText());
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
            public void enterDocMetricAtomExtract(JQLParser.DocMetricAtomExtractContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                final String regex = ParserCommon.unquote(ctx.regex.getText());
                final int groupNumber;
                if (ctx.groupNumber != null) {
                    groupNumber = Integer.parseInt(ctx.groupNumber.getText());
                } else {
                    groupNumber = 1;
                }
                accept(scopedField.wrap(new DocMetric.Extract(scopedField.field, regex, groupNumber)));
            }

            @Override
            public void enterSyntacticallyAtomicDocMetricAtom(JQLParser.SyntacticallyAtomicDocMetricAtomContext ctx) {
                accept(parseJQLSyntacticallyAtomicDocMetricAtom(ctx.jqlSyntacticallyAtomicDocMetricAtom()));
            }

            @Override
            public void enterDocMetricAtomLucene(final JQLParser.DocMetricAtomLuceneContext ctx) {
                accept(new DocMetric.Lucene(ParserCommon.unquote(ctx.queryField.getText()), datasetToKeywordAnalyzerFields, datasetToIntFields));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled jql doc metric atom: [" + jqlDocMetricAtomContext.getText() + "]");
        }

        ref[0].copyPosition(jqlDocMetricAtomContext);

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
