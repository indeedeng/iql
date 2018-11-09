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

import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;
import com.indeed.iql2.language.util.ValidationUtil;

import javax.annotation.Nullable;
import java.util.List;

public class DocMetrics {
    public static DocMetric parseDocMetric(
            final JQLParser.DocMetricContext metricContext,
            final Query.Context context) {
        if (metricContext.jqlDocMetric() != null) {
            return parseJQLDocMetric(metricContext.jqlDocMetric(), context);
        }
        if (metricContext.legacyDocMetric() != null) {
            return parseLegacyDocMetric(metricContext.legacyDocMetric(), context.fieldResolver, context.datasetsMetadata);
        }
        throw new UnsupportedOperationException("What do?!");
    }

    public static DocMetric parseLegacyDocMetric(JQLParser.LegacyDocMetricContext legacyDocMetricContext, final ScopedFieldResolver fieldResolver, final DatasetsMetadata datasetsMetadata) {
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
                accept(new DocMetric.Signum(parseLegacyDocMetric(ctx.legacyDocMetric(), fieldResolver, datasetsMetadata)));
            }

            @Override
            public void enterLegacyDocPlusOrMinus(JQLParser.LegacyDocPlusOrMinusContext ctx) {
                final DocMetric left = parseLegacyDocMetric(ctx.legacyDocMetric(0), fieldResolver, datasetsMetadata);
                final DocMetric right = parseLegacyDocMetric(ctx.legacyDocMetric(1), fieldResolver, datasetsMetadata);
                if (ctx.plus != null) {
                    accept(new DocMetric.Add(left, right));
                } else if (ctx.minus != null) {
                    accept(new DocMetric.Subtract(left, right));
                }
            }

            @Override
            public void enterLegacyDocMultOrDivideOrModulus(JQLParser.LegacyDocMultOrDivideOrModulusContext ctx) {
                final DocMetric left = parseLegacyDocMetric(ctx.legacyDocMetric(0), fieldResolver, datasetsMetadata);
                final DocMetric right = parseLegacyDocMetric(ctx.legacyDocMetric(1), fieldResolver, datasetsMetadata);
                if (ctx.multiply != null) {
                    accept(new DocMetric.Multiply(left, right));
                } else if (ctx.divide != null) {
                    accept(new DocMetric.Divide(left, right));
                } else if (ctx.modulus != null) {
                    accept(new DocMetric.Modulus(left, right));
                }
            }

            public void enterLegacyDocMetricParens(JQLParser.LegacyDocMetricParensContext ctx) {
                accept(parseLegacyDocMetric(ctx.legacyDocMetric(), fieldResolver, datasetsMetadata));
            }

            public void enterLegacyDocAbs(JQLParser.LegacyDocAbsContext ctx) {
                accept(new DocMetric.Abs(parseLegacyDocMetric(ctx.legacyDocMetric(), fieldResolver, datasetsMetadata)));
            }

            public void enterLegacyDocNegate(JQLParser.LegacyDocNegateContext ctx) {
                accept(new DocMetric.Negate(parseLegacyDocMetric(ctx.legacyDocMetric(), fieldResolver, datasetsMetadata)));
            }

            public void enterLegacyDocInt(JQLParser.LegacyDocIntContext ctx) {
                accept(new DocMetric.Constant(Long.parseLong(ctx.integer().getText())));
            }

            @Override
            public void enterLegacyDocInequality(JQLParser.LegacyDocInequalityContext ctx) {
                final DocMetric left = parseLegacyDocMetric(ctx.legacyDocMetric(0), fieldResolver, datasetsMetadata);
                final DocMetric right = parseLegacyDocMetric(ctx.legacyDocMetric(1), fieldResolver, datasetsMetadata);
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
                accept(new DocMetric.Log(parseLegacyDocMetric(ctx.legacyDocMetric(), fieldResolver, datasetsMetadata), scaleFactor));
            }

            @Override
            public void enterLegacyDocExp(JQLParser.LegacyDocExpContext ctx) {
                final int scaleFactor = ctx.integer() == null ? 1 : Integer.parseInt(ctx.integer().getText());
                accept(new DocMetric.Exponentiate(parseLegacyDocMetric(ctx.legacyDocMetric(), fieldResolver, datasetsMetadata), scaleFactor));
            }

            @Override
            public void enterLegacyDocMin(JQLParser.LegacyDocMinContext ctx) {
                accept(new DocMetric.Min(parseLegacyDocMetric(ctx.arg1, fieldResolver, datasetsMetadata), parseLegacyDocMetric(ctx.arg2, fieldResolver, datasetsMetadata)));
            }

            @Override
            public void enterLegacyDocMax(JQLParser.LegacyDocMaxContext ctx) {
                accept(new DocMetric.Max(parseLegacyDocMetric(ctx.arg1, fieldResolver, datasetsMetadata), parseLegacyDocMetric(ctx.arg2, fieldResolver, datasetsMetadata)));
            }

            public void enterLegacyDocAtom(JQLParser.LegacyDocAtomContext ctx) {
                accept(parseLegacyDocMetricAtom(ctx.legacyDocMetricAtom(), fieldResolver, datasetsMetadata));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc metric: [" + legacyDocMetricContext.getText() + "]");
        }

        ref[0].copyPosition(legacyDocMetricContext);

        return ref[0];
    }

    public static DocMetric parseLegacyDocMetricAtom(
            final JQLParser.LegacyDocMetricAtomContext legacyDocMetricAtomContext,
            final ScopedFieldResolver fieldResolver, final DatasetsMetadata datasetsMetadata
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
                accept(new DocMetric.HasString(fieldResolver.resolve(ctx.field), ParserCommon.unquote(ctx.term.getText())));
            }

            @Override
            public void enterLegacyDocMetricAtomHasntString(JQLParser.LegacyDocMetricAtomHasntStringContext ctx) {
                accept(negateMetric(new DocMetric.HasString(fieldResolver.resolve(ctx.field), ParserCommon.unquote(ctx.term.getText()))));
            }

            @Override
            public void enterLegacyDocMetricAtomHasInt(JQLParser.LegacyDocMetricAtomHasIntContext ctx) {
                final long term = Long.parseLong(ctx.integer().getText());
                accept(fieldResolver.resolveDocMetric(ctx.field, new ScopedFieldResolver.HasIntCallback(term)));
            }

            @Override
            public void enterLegacyDocMetricAtomHasntInt(JQLParser.LegacyDocMetricAtomHasntIntContext ctx) {
                final long term = Long.parseLong(ctx.integer().getText());
                accept(fieldResolver.resolveDocMetric(ctx.field, new ScopedFieldResolver.HasIntCallback(term).map(DocMetrics::negateMetric)));
            }

            @Override
            public void enterLegacyDocMetricAtomHasStringQuoted(JQLParser.LegacyDocMetricAtomHasStringQuotedContext ctx) {
                final HasTermQuote hasTermQuote = HasTermQuote.create(ctx.STRING_LITERAL().getText());
                final FieldSet field = fieldResolver.resolveContextless(hasTermQuote.getField());
                accept(new DocMetric.HasString(field, hasTermQuote.getTerm()));
            }

            @Override
            public void enterLegacyDocMetricAtomHasIntQuoted(JQLParser.LegacyDocMetricAtomHasIntQuotedContext ctx) {
                final HasTermQuote hasTermQuote = HasTermQuote.create(ctx.STRING_LITERAL().getText());
                final long termInt = Long.parseLong(hasTermQuote.getTerm());
                accept(fieldResolver.resolveDocMetric(Queries.runParser(hasTermQuote.getField(), JQLParser::identifierTerminal).identifier(), new ScopedFieldResolver.HasIntCallback(termInt)));
            }

            @Override
            public void enterLegacyDocMetricAtomFloatScale(JQLParser.LegacyDocMetricAtomFloatScaleContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.field);
                final double mult = ctx.mult == null ? 1.0 : Double.parseDouble(ctx.mult.getText());
                final double add = ctx.add == null ? 0.0 : Double.parseDouble(ctx.add.getText());
                accept(new DocMetric.FloatScale(field, mult, add));
            }

            @Override
            public void enterLegacyDocMetricAtomRawField(JQLParser.LegacyDocMetricAtomRawFieldContext ctx) {
                accept(fieldResolver.resolveDocMetric(ctx.identifier(), ScopedFieldResolver.PLAIN_DOC_METRIC_CALLBACK));
            }

            @Override
            public void enterLegacyDocMetricAtomLucene(final JQLParser.LegacyDocMetricAtomLuceneContext ctx) {
                accept(new DocMetric.Lucene(ParserCommon.unquote(ctx.queryField.getText()), datasetsMetadata, fieldResolver));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled legacy doc metric atom: [" + legacyDocMetricAtomContext.getText() + "]");
        }

        ref[0].copyPosition(legacyDocMetricAtomContext);

        return ref[0];
    }

    // .. this used to be more substantial. TODO: Inline this grammar rule?
    public static DocMetric parseJQLSyntacticallyAtomicDocMetricAtom(JQLParser.JqlSyntacticallyAtomicDocMetricAtomContext jqlSyntacticallyAtomicDocMetricAtomContext, final ScopedFieldResolver fieldResolver) {
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
                accept(fieldResolver.resolveDocMetric(ctx.singlyScopedField(), ScopedFieldResolver.PLAIN_DOC_METRIC_CALLBACK));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled jql syntactically atomic doc metric: [" + jqlSyntacticallyAtomicDocMetricAtomContext.getText() + "]");
        }

        ref[0].copyPosition(jqlSyntacticallyAtomicDocMetricAtomContext);

        return ref[0];
    }

    public static DocMetric parseJQLDocMetric(
            final JQLParser.JqlDocMetricContext metricContext,
            final Query.Context context) {
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
                accept(new DocMetric.Signum(parseJQLDocMetric(ctx.jqlDocMetric(), context)));
            }

            @Override
            public void enterDocPlusOrMinus(JQLParser.DocPlusOrMinusContext ctx) {
                final DocMetric left = parseJQLDocMetric(ctx.jqlDocMetric(0), context);
                final DocMetric right = parseJQLDocMetric(ctx.jqlDocMetric(1), context);
                if (ctx.plus != null) {
                    accept(new DocMetric.Add(left, right));
                } else if (ctx.minus != null) {
                    accept(new DocMetric.Subtract(left, right));
                }
            }

            @Override
            public void enterDocMultOrDivideOrModulus(JQLParser.DocMultOrDivideOrModulusContext ctx) {
                final DocMetric left = parseJQLDocMetric(ctx.jqlDocMetric(0), context);
                final DocMetric right = parseJQLDocMetric(ctx.jqlDocMetric(1), context);
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
                final DocMetric left = parseJQLDocMetric(ctx.jqlDocMetric(0), context);
                final DocMetric right = parseJQLDocMetric(ctx.jqlDocMetric(1), context);
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
                accept(parseJQLDocMetric(ctx.jqlDocMetric(), context));
            }

            public void enterDocAbs(JQLParser.DocAbsContext ctx) {
                accept(new DocMetric.Abs(parseJQLDocMetric(ctx.jqlDocMetric(), context)));
            }

            public void enterDocNegate(JQLParser.DocNegateContext ctx) {
                accept(new DocMetric.Negate(parseJQLDocMetric(ctx.jqlDocMetric(), context)));
            }

            public void enterDocIfThenElse(JQLParser.DocIfThenElseContext ctx) {
                final DocFilter condition = DocFilters.parseJQLDocFilter(ctx.jqlDocFilter(), context);
                final DocMetric trueCase = parseJQLDocMetric(ctx.trueCase, context);
                final DocMetric falseCase = parseJQLDocMetric(ctx.falseCase, context);
                accept(new DocMetric.IfThenElse(condition, trueCase, falseCase));
            }

            public void enterDocInt(JQLParser.DocIntContext ctx) {
                accept(new DocMetric.Constant(Long.parseLong(ctx.integer().getText())));
            }

            @Override
            public void enterDocLog(JQLParser.DocLogContext ctx) {
                final int scaleFactor = ctx.integer() == null ? 1 : Integer.parseInt(ctx.integer().getText());
                accept(new DocMetric.Log(parseJQLDocMetric(ctx.jqlDocMetric(), context), scaleFactor));
            }

            @Override
            public void enterDocExp(JQLParser.DocExpContext ctx) {
                final int scaleFactor = ctx.integer() == null ? 1 : Integer.parseInt(ctx.integer().getText());
                accept(new DocMetric.Exponentiate(parseJQLDocMetric(ctx.jqlDocMetric(), context), scaleFactor));
            }

            @Override
            public void enterDocMin(JQLParser.DocMinContext ctx) {
                DocMetric resultMetric = parseJQLDocMetric(ctx.metrics.get(0), context);
                List<JQLParser.JqlDocMetricContext> metrics = ctx.metrics;
                for (int i = 1; i < metrics.size(); i++) {
                    resultMetric = new DocMetric.Min(resultMetric, parseJQLDocMetric(metrics.get(i), context));
                }
                accept(resultMetric);
            }

            @Override
            public void enterDocMax(JQLParser.DocMaxContext ctx) {
                DocMetric resultMetric = parseJQLDocMetric(ctx.metrics.get(0), context);
                List<JQLParser.JqlDocMetricContext> metrics = ctx.metrics;
                for (int i = 1; i < metrics.size(); i++) {
                    resultMetric = new DocMetric.Max(resultMetric, parseJQLDocMetric(metrics.get(i), context));
                }
                accept(resultMetric);
            }

            @Override
            public void enterDocAtom(JQLParser.DocAtomContext ctx) {
                accept(parseJQLDocMetricAtom(ctx.jqlDocMetricAtom(), context.fieldResolver, context.datasetsMetadata));
            }

            @Override
            public void enterDocMetricFilter(JQLParser.DocMetricFilterContext ctx) {
                final DocFilter filter = DocFilters.parseJQLDocFilter(ctx.jqlDocFilter(), context);
                accept(new DocMetric.IfThenElse(filter, new DocMetric.Constant(1), new DocMetric.Constant(0)));
            }

            @Override
            public void enterDocId(final JQLParser.DocIdContext ctx) {
                accept(new DocMetric.DocId());
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
            final ScopedFieldResolver fieldResolver, final DatasetsMetadata datasetsMetadata) {
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
                final long term = Long.parseLong(ctx.integer().getText());
                accept(fieldResolver.resolveDocMetric(ctx.singlyScopedField(), new ScopedFieldResolver.HasIntCallback(term).map(DocMetrics::negateMetric)));
            }

            @Override
            public void enterDocMetricAtomHasntString(JQLParser.DocMetricAtomHasntStringContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                accept(field.wrap(negateMetric(new DocMetric.HasString(field, ParserCommon.unquote(ctx.term.getText())))));
            }

            @Override
            public void enterDocMetricAtomHasString(JQLParser.DocMetricAtomHasStringContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                accept(field.wrap(new DocMetric.HasString(field, ParserCommon.unquote(ctx.term.getText()))));
            }

            @Override
            public void enterDocMetricAtomHasInt(JQLParser.DocMetricAtomHasIntContext ctx) {
                final long term = Long.parseLong(ctx.integer().getText());
                accept(fieldResolver.resolveDocMetric(ctx.singlyScopedField(), new ScopedFieldResolver.HasIntCallback(term)));
            }

            @Override
            public void enterDocMetricAtomFloatScale(JQLParser.DocMetricAtomFloatScaleContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                final double mult = ctx.mult == null ? 1.0 : Double.parseDouble(ctx.mult.getText());
                final double add = ctx.add == null ? 0.0 : Double.parseDouble(ctx.add.getText());
                accept(field.wrap(new DocMetric.FloatScale(field, mult, add)));
            }

            @Override
            public void enterDocMetricAtomHasIntField(JQLParser.DocMetricAtomHasIntFieldContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                accept(field.wrap(new DocMetric.HasIntField(field)));
            }

            @Override
            public void enterDocMetricAtomHasStringField(JQLParser.DocMetricAtomHasStringFieldContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                accept(field.wrap(new DocMetric.HasStringField(field)));
            }

            @Override
            public void enterDocMetricAtomIntTermCount(JQLParser.DocMetricAtomIntTermCountContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                accept(field.wrap(new DocMetric.IntTermCount(field)));
            }

            @Override
            public void enterDocMetricAtomStrTermCount(JQLParser.DocMetricAtomStrTermCountContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                accept(field.wrap(new DocMetric.StrTermCount(field)));
            }

            @Override
            public void enterDocMetricAtomRegex(JQLParser.DocMetricAtomRegexContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                // TODO: How to handle regex parsing? Same as Java?
                accept(field.wrap(new DocMetric.RegexMetric(field, ParserCommon.unquote(ctx.regex.getText()))));
            }

            @Override
            public void enterDocMetricAtomExtract(JQLParser.DocMetricAtomExtractContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                // TODO: How to handle regex parsing? Same as Java?
                final String regex = ParserCommon.unquote(ctx.regex.getText());
                final int groupNumber;
                if (ctx.groupNumber != null) {
                    groupNumber = Integer.parseInt(ctx.groupNumber.getText());
                } else {
                    groupNumber = 1;
                }
                accept(field.wrap(new DocMetric.Extract(field, regex, groupNumber)));
            }

            @Override
            public void enterSyntacticallyAtomicDocMetricAtom(JQLParser.SyntacticallyAtomicDocMetricAtomContext ctx) {
                accept(parseJQLSyntacticallyAtomicDocMetricAtom(ctx.jqlSyntacticallyAtomicDocMetricAtom(), fieldResolver));
            }

            @Override
            public void enterDocMetricAtomLucene(final JQLParser.DocMetricAtomLuceneContext ctx) {
                accept(new DocMetric.Lucene(ParserCommon.unquote(ctx.queryField.getText()), datasetsMetadata, fieldResolver));
            }

            @Override
            public void enterDocMetricAtomFieldEqual(final JQLParser.DocMetricAtomFieldEqualContext ctx) {
                final DocMetric metric1 = extractPlainDimensionDocMetric(ctx.singlyScopedField(0), fieldResolver);
                final DocMetric metric2 = extractPlainDimensionDocMetric(ctx.singlyScopedField(1), fieldResolver);

                final FieldSet plainField1 = DocFilters.extractPlainField(metric1);
                final FieldSet plainField2 = DocFilters.extractPlainField(metric2);

                final DocMetric result;
                if ((plainField1 != null) && (plainField2 != null)) {
                    ValidationUtil.validateSameScopeThrowException(plainField1, plainField2);
                    result = plainField1.wrap(new DocMetric.FieldEqualMetric(plainField1, plainField2));
                } else {
                    ValidationUtil.validateSameQualifieds(ctx, metric1, metric2);
                    result = new DocMetric.MetricEqual(metric1, metric2);
                }

                accept(result);
            }

            @Override
            public void enterDocMetricAtomNotFieldEqual(final JQLParser.DocMetricAtomNotFieldEqualContext ctx) {
                final DocMetric metric1 = extractPlainDimensionDocMetric(ctx.singlyScopedField(0), fieldResolver);
                final DocMetric metric2 = extractPlainDimensionDocMetric(ctx.singlyScopedField(1), fieldResolver);

                final FieldSet plainField1 = DocFilters.extractPlainField(metric1);
                final FieldSet plainField2 = DocFilters.extractPlainField(metric2);

                final DocMetric result;
                if ((plainField1 != null) && (plainField2 != null)) {
                    ValidationUtil.validateSameScopeThrowException(plainField1, plainField2);
                    result = plainField1.wrap(negateMetric(new DocMetric.FieldEqualMetric(plainField1, plainField2)));
                } else {
                    ValidationUtil.validateSameQualifieds(ctx, metric1, metric2);
                    result = new DocMetric.MetricNotEqual(metric1, metric2);
                }

                accept(result);
            }

            @Override
            public void enterDocMetricAtomLen(final JQLParser.DocMetricAtomLenContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                accept(field.wrap(new DocMetric.StringLen(field)));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled jql doc metric atom: [" + jqlDocMetricAtomContext.getText() + "]");
        }

        ref[0].copyPosition(jqlDocMetricAtomContext);

        return ref[0];
    }

    static DocMetric extractPlainDimensionDocMetric(final JQLParser.SinglyScopedFieldContext field, final ScopedFieldResolver fieldResolver) {
        return fieldResolver.resolveDocMetric(field, ScopedFieldResolver.PLAIN_DOC_METRIC_CALLBACK);
    }

    public static DocMetric negateMetric(DocMetric metric) {
        return new DocMetric.Subtract(new DocMetric.Constant(1), metric);
    }

    @Nullable
    public static JQLParser.SinglyScopedFieldContext asPlainField(final JQLParser.JqlSyntacticallyAtomicDocMetricAtomContext ctx) {
        if (ctx instanceof JQLParser.DocMetricAtomRawFieldContext) {
            final JQLParser.DocMetricAtomRawFieldContext ctx2 = (JQLParser.DocMetricAtomRawFieldContext) ctx;
            return ctx2.singlyScopedField();
        }
        return null;
    }

    @Nullable
    public static JQLParser.SinglyScopedFieldContext asPlainField(final JQLParser.JqlDocMetricAtomContext ctx) {
        if (ctx instanceof JQLParser.SyntacticallyAtomicDocMetricAtomContext) {
            return asPlainField(((JQLParser.SyntacticallyAtomicDocMetricAtomContext) ctx).jqlSyntacticallyAtomicDocMetricAtom());
        }
        return null;
    }

    @Nullable
    public static JQLParser.SinglyScopedFieldContext asPlainField(final JQLParser.JqlDocMetricContext ctx) {
        if (ctx instanceof JQLParser.DocAtomContext) {
            return asPlainField(((JQLParser.DocAtomContext) ctx).jqlDocMetricAtom());
        }
        return null;
    }

    @Nullable
    public static JQLParser.IdentifierContext asPlainField(final JQLParser.LegacyDocMetricContext ctx) {
        if (ctx instanceof JQLParser.LegacyDocAtomContext) {
            final JQLParser.LegacyDocMetricAtomContext ctx2 = ((JQLParser.LegacyDocAtomContext) ctx).legacyDocMetricAtom();
            if (ctx2 instanceof JQLParser.LegacyDocMetricAtomRawFieldContext) {
                final JQLParser.LegacyDocMetricAtomRawFieldContext ctx3 = (JQLParser.LegacyDocMetricAtomRawFieldContext) ctx2;
                return ctx3.identifier();
            }
        }
        return null;
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
