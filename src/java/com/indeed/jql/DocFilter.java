package com.indeed.jql;

import org.antlr.v4.runtime.misc.NotNull;

import java.util.List;
import java.util.function.BiFunction;

public interface DocFilter {
    static DocFilter and(List<DocFilter> filters) {
        if (filters == null || filters.isEmpty()) {
            return new Always();
        }
        if (filters.size() == 1) {
            return filters.get(0);
        }
        DocFilter result = filters.get(0);
        for (int i = 1; i < filters.size(); i++) {
            result = new And(filters.get(i), result);
        }
        return result;
    }

    static DocFilter parseDocFilter(JQLParser.DocFilterContext docFilterContext) {
        final DocFilter[] ref = new DocFilter[1];

        docFilterContext.enterRule(new JQLBaseListener() {
            public void accept(DocFilter value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            @Override
            public void enterDocBetween(@NotNull JQLParser.DocBetweenContext ctx) {
                final String field = ctx.field.getText();
                final long lowerBound = Long.parseLong(ctx.lowerBound.getText());
                final long upperBound = Long.parseLong(ctx.upperBound.getText());
                accept(new Between(field, lowerBound, upperBound));
            }

            @Override
            public void enterDocFieldIn(@NotNull JQLParser.DocFieldInContext ctx) {
                String field = ctx.field.getText();
                final List<JQLParser.TermValContext> terms = ctx.terms;
                final boolean negate = ctx.not != null;
                accept(docInHelper(field, terms, negate));
            }

            @Override
            public void enterDocFieldIsnt(@NotNull JQLParser.DocFieldIsntContext ctx) {
                new FieldIsnt(ctx.field.getText(), Main.parseTerm(ctx.termVal()));
            }

            @Override
            public void enterDocSample(@NotNull JQLParser.DocSampleContext ctx) {
                final String field = ctx.field.getText();
                final long numerator = Long.parseLong(ctx.numerator.getText());
                final long denominator = Long.parseLong(ctx.denominator.getText());
                final String seed = ctx.seed.getText();
                accept(new Sample(field, numerator, denominator, seed));
            }

            @Override
            public void enterDocNot(@NotNull JQLParser.DocNotContext ctx) {
                accept(new Not(parseDocFilter(ctx.docFilter())));
            }

            @Override
            public void enterDocRegex(@NotNull JQLParser.DocRegexContext ctx) {
                accept(new Regex(ctx.field.getText(), Main.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterDocFieldIs(@NotNull JQLParser.DocFieldIsContext ctx) {
                accept(new FieldIs(ctx.field.getText(), Main.parseTerm(ctx.termVal())));
            }

            @Override
            public void enterDocOr(@NotNull JQLParser.DocOrContext ctx) {
                accept(new Or(parseDocFilter(ctx.docFilter(0)), parseDocFilter(ctx.docFilter(1))));
            }

            @Override
            public void enterDocTrue(@NotNull JQLParser.DocTrueContext ctx) {
                accept(new Always());
            }

            @Override
            public void enterDocMetricInequality(@NotNull JQLParser.DocMetricInequalityContext ctx) {
                final String op = ctx.op.getText();
                final BiFunction<DocMetric, DocMetric, DocFilter> f;
                switch (op) {
                    case "=": {
                        f = DocFilter.MetricEqual::new;
                        break;
                    }
                    case "!=": {
                        f = DocFilter.MetricNotEqual::new;
                        break;
                    }
                    case "<": {
                        f = DocFilter.MetricLt::new;
                        break;
                    }
                    case "<=": {
                        f = DocFilter.MetricLte::new;
                        break;
                    }
                    case ">": {
                        f = DocFilter.MetricGt::new;
                        break;
                    }
                    case ">=": {
                        f = DocFilter.MetricGte::new;
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Unknown doc metric operator: " + op);
                }
                accept(f.apply(DocMetric.parseDocMetric(ctx.docMetric(0)), DocMetric.parseDocMetric(ctx.docMetric(1))));
            }

            @Override
            public void enterDocAnd(@NotNull JQLParser.DocAndContext ctx) {
                accept(new And(parseDocFilter(ctx.docFilter(0)), parseDocFilter(ctx.docFilter(1))));
            }

            @Override
            public void enterLucene(@NotNull JQLParser.LuceneContext ctx) {
                accept(new Lucene(Main.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterDocNotRegex(@NotNull JQLParser.DocNotRegexContext ctx) {
                accept(new NotRegex(ctx.field.getText(), Main.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterDocFilterParens(@NotNull JQLParser.DocFilterParensContext ctx) {
                accept(parseDocFilter(ctx.docFilter()));
            }

            @Override
            public void enterDocFalse(@NotNull JQLParser.DocFalseContext ctx) {
                accept(new Never());
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc filter: [" + docFilterContext.getText() + "]");
        }
        return ref[0];
    }

    static DocFilter docInHelper(String field, List<JQLParser.TermValContext> terms, boolean negate) {
        DocFilter filter = null;
        for (final JQLParser.TermValContext term : terms) {
            if (filter == null) {
                filter = new FieldIs(field, Main.parseTerm(term));
            } else {
                filter = new Or(new FieldIs(field, Main.parseTerm(term)), filter);
            }
        }
        if (filter == null) {
            // TODO (optional): Make this new Always() and don't negate if (ctx.not != null).
            filter = new Never();
        }
        if (negate) {
            filter = new Not(filter);
        }
        return filter;
    }

    class FieldIs implements DocFilter {
        private final String field;
        private final Term term;

        public FieldIs(String field, Term term) {
            this.field = field;
            this.term = term;
        }
    }

    class FieldIsnt implements DocFilter {
        private final String field;
        private final Term term;

        public FieldIsnt(String field, Term term) {
            this.field = field;
            this.term = term;
        }
    }

    class Between implements DocFilter {
        private final String field;
        private final long lowerBound;
        private final long upperBound;

        public Between(String field, long lowerBound, long upperBound) {
            this.field = field;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }
    }

    class MetricEqual implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricEqual(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class MetricNotEqual implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricNotEqual(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class MetricGt implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricGt(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class MetricGte implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricGte(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class MetricLt implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricLt(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class MetricLte implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricLte(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class And implements DocFilter {
        private final DocFilter f1;
        private final DocFilter f2;

        public And(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }
    }

    class Or implements DocFilter {
        private final DocFilter f1;
        private final DocFilter f2;

        public Or(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }
    }

    class Not implements DocFilter {
        private final DocFilter f;

        public Not(DocFilter f) {
            this.f = f;
        }
    }

    class Regex implements DocFilter {
        private final String field;
        private final String regex;

        public Regex(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }
    }

    class NotRegex implements DocFilter {
        private final String field;
        private final String regex;

        public NotRegex(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }
    }

    class Qualified implements DocFilter {
        private final List<String> scope;
        private final DocFilter filter;

        public Qualified(List<String> scope, DocFilter filter) {
            this.scope = scope;
            this.filter = filter;
        }
    }

    class Lucene implements DocFilter {
        private final String query;

        public Lucene(String query) {
            this.query = query;
        }
    }

    class Sample implements DocFilter {
        private final String field;
        private final long numerator;
        private final long denominator;
        private final String seed;

        public Sample(String field, long numerator, long denominator, String seed) {
            this.field = field;
            this.numerator = numerator;
            this.denominator = denominator;
            this.seed = seed;
        }
    }

    class Always implements DocFilter {}
    class Never implements DocFilter {}
}
