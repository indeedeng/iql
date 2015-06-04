package com.indeed.jql;

import org.antlr.v4.runtime.misc.NotNull;

public interface DocMetric {
    static DocMetric parseDocMetric(JQLParser.DocMetricContext metricContext) {
        final DocMetric[] ref = new DocMetric[1];

        metricContext.enterRule(new JQLBaseListener() {
            private void accept(DocMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterDocCounts(@NotNull JQLParser.DocCountsContext ctx) {
                accept(new Field("count()"));
            }

            public void enterDocSignum(@NotNull JQLParser.DocSignumContext ctx) {
                accept(new Signum(parseDocMetric(ctx.docMetric())));
            }

            public void enterDocHasIntQuoted(@NotNull JQLParser.DocHasIntQuotedContext ctx) {
                final Main.HasTermQuote hasTermQuote = new Main.HasTermQuote(ctx.STRING_LITERAL().getText());
                final long termInt = Long.parseLong(hasTermQuote.getTerm());
                accept(new HasInt(hasTermQuote.getField(), termInt));
            }

            public void enterDocMinus(@NotNull JQLParser.DocMinusContext ctx) {
                accept(new Subtract(parseDocMetric(ctx.docMetric(0)), parseDocMetric(ctx.docMetric(1))));
            }

            public void enterDocMod(@NotNull JQLParser.DocModContext ctx) {
                accept(new Modulus(parseDocMetric(ctx.docMetric(0)), parseDocMetric(ctx.docMetric(1))));
            }

            public void enterDocPlus(@NotNull JQLParser.DocPlusContext ctx) {
                accept(new Add(parseDocMetric(ctx.docMetric(0)), parseDocMetric(ctx.docMetric(1))));
            }

            public void enterDocHasStringQuoted(@NotNull JQLParser.DocHasStringQuotedContext ctx) {
                final Main.HasTermQuote hasTermQuote = new Main.HasTermQuote(ctx.STRING_LITERAL().getText()).invoke();
                accept(new HasString(hasTermQuote.getField(), hasTermQuote.getTerm()));
            }

            public void enterDocHasInt(@NotNull JQLParser.DocHasIntContext ctx) {
                final String field = ctx.field.getText();
                final long term = Long.parseLong(ctx.INT().getText());
                accept(new HasInt(field, term));
            }

            public void enterDocRawField(@NotNull JQLParser.DocRawFieldContext ctx) {
                accept(new Field(ctx.identifier().getText()));
            }

            public void enterDocMetricParens(@NotNull JQLParser.DocMetricParensContext ctx) {
                accept(parseDocMetric(ctx.docMetric()));
            }

            public void enterDocFloatScale(@NotNull JQLParser.DocFloatScaleContext ctx) {
                final String field = ctx.field.getText();
                final double mult = Double.parseDouble(ctx.mult.getText());
                final double add = Double.parseDouble(ctx.add.getText());
                accept(new FloatScale(field, mult, add));
            }

            public void enterDocHasString(@NotNull JQLParser.DocHasStringContext ctx) {
                accept(new HasString(ctx.field.getText(), ctx.term.getText()));
            }

            public void enterDocDiv(@NotNull JQLParser.DocDivContext ctx) {
                accept(new Divide(parseDocMetric(ctx.docMetric(0)), parseDocMetric(ctx.docMetric(1))));
            }

            public void enterDocAbs(@NotNull JQLParser.DocAbsContext ctx) {
                accept(new Abs(parseDocMetric(ctx.docMetric())));
            }

            public void enterDocNegate(@NotNull JQLParser.DocNegateContext ctx) {
                accept(new Negate(parseDocMetric(ctx.docMetric())));
            }

            public void enterDocHasntInt(@NotNull JQLParser.DocHasntIntContext ctx) {
                final String field = ctx.field.getText();
                final long term = Long.parseLong(ctx.INT().getText());
                accept(negateMetric(new HasInt(field, term)));
            }

            public void enterDocHasntString(@NotNull JQLParser.DocHasntStringContext ctx) {
                accept(negateMetric(new HasString(ctx.field.getText(), ctx.term.getText())));
            }

            public void enterDocIfThenElse(@NotNull JQLParser.DocIfThenElseContext ctx) {
                final DocFilter condition = DocFilter.parseDocFilter(ctx.docFilter());
                final DocMetric trueCase = parseDocMetric(ctx.trueCase);
                final DocMetric falseCase = parseDocMetric(ctx.falseCase);
                accept(new IfThenElse(condition, trueCase, falseCase));
            }

            public void enterDocInt(@NotNull JQLParser.DocIntContext ctx) {
                accept(new Constant(Long.parseLong(ctx.INT().getText())));
            }

            public void enterDocMult(@NotNull JQLParser.DocMultContext ctx) {
                accept(new Multiply(parseDocMetric(ctx.docMetric(0)), parseDocMetric(ctx.docMetric(1))));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc metric: [" + metricContext.getText() + "]");
        }
        return ref[0];
    }

    static DocMetric negateMetric(DocMetric metric) {
        return new Subtract(new Constant(1), metric);
    }

    class Field implements DocMetric {
        private final String field;

        public Field(String field) {
            this.field = field;
        }
    }

    class Unop implements DocMetric {
        private final DocMetric m1;

        public Unop(DocMetric m1) {
            this.m1 = m1;
        }
    }

    class Log extends Unop {
        public Log(DocMetric m1) {
            super(m1);
        }
    }

    class Negate extends Unop {
        public Negate(DocMetric m1) {
            super(m1);
        }
    }

    class Abs extends Unop {
        public Abs(DocMetric m1) {
            super(m1);
        }
    }

    class Signum extends Unop {
        public Signum(DocMetric m1) {
            super(m1);
        }
    }

    class Binop implements DocMetric {
        private final DocMetric m1;
        private final DocMetric m2;

        public Binop(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class Add extends Binop {
        public Add(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class Subtract extends Binop {
        public Subtract(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class Multiply extends Binop {
        public Multiply(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class Divide extends Binop {
        public Divide(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class Modulus extends Binop {
        public Modulus(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class Min extends Binop {
        public Min(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class Max extends Binop {
        public Max(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class MetricEqual extends Binop {
        public MetricEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class MetricNotEqual extends Binop {
        public MetricNotEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class MetricLt extends Binop {
        public MetricLt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class MetricLte extends Binop {
        public MetricLte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class MetricGt extends Binop {
        public MetricGt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class MetricGte extends Binop {
        public MetricGte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class RegexMetric implements DocMetric {
        private final String field;
        private final String regex;

        public RegexMetric(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }
    }

    class FloatScale implements DocMetric {
        private final String field;
        private final double mult;
        private final double add;

        public FloatScale(String field, double mult, double add) {
            this.field = field;
            this.mult = mult;
            this.add = add;
        }
    }

    class Constant implements DocMetric {
        private final long value;

        public Constant(long value) {
            this.value = value;
        }
    }

    class HasInt implements DocMetric {
        private final String field;
        private final long term;

        public HasInt(String field, long term) {
            this.field = field;
            this.term = term;
        }
    }

    class HasString implements DocMetric {
        private final String field;
        private final String term;

        public HasString(String field, String term) {
            this.field = field;
            this.term = term;
        }
    }

    class IfThenElse implements DocMetric {
        private final DocFilter condition;
        private final DocMetric trueCase;
        private final DocMetric falseCase;

        public IfThenElse(DocFilter condition, DocMetric trueCase, DocMetric falseCase) {
            this.condition = condition;
            this.trueCase = trueCase;
            this.falseCase = falseCase;
        }
    }
}
