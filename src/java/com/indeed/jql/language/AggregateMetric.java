package com.indeed.jql.language;

import com.google.common.base.Function;
import com.google.common.base.Optional;

import java.util.List;
import java.util.Objects;

public interface AggregateMetric {

    AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i);
    AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f);

    abstract class Unop implements AggregateMetric {
        public final AggregateMetric m1;

        public Unop(AggregateMetric m1) {
            this.m1 = m1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Unop unop = (Unop) o;
            return Objects.equals(m1, unop.m1);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1);
        }
    }

    class Log extends Unop {
        public Log(AggregateMetric m1) {
            super(m1);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Log(m1.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Log(f.apply(m1));
        }
    }

    class Negate extends Unop {
        public Negate(AggregateMetric m1) {
            super(m1);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Negate(m1.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Negate(f.apply(m1));
        }
    }

    class Abs extends Unop {
        public Abs(AggregateMetric m1) {
            super(m1);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Abs(m1.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Abs(f.apply(m1));
        }
    }

    abstract class Binop implements AggregateMetric {
        public final AggregateMetric m1;
        public final AggregateMetric m2;

        public Binop(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Binop binop = (Binop) o;
            return Objects.equals(m1, binop.m1) &&
                    Objects.equals(m2, binop.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }
    }

    class Add extends Binop {
        public Add(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Add(m1.transform(f, g, h, i), m2.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Add(f.apply(m1), f.apply(m2));
        }
    }

    class Subtract extends Binop {
        public Subtract(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Subtract(m1.transform(f, g, h, i), m2.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Subtract(f.apply(m1), f.apply(m2));
        }
    }

    class Multiply extends Binop {
        public Multiply(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Multiply(m1.transform(f, g, h, i), m2.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Multiply(f.apply(m1), f.apply(m2));
        }
    }

    class Divide extends Binop {
        public Divide(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Divide(m1.transform(f, g, h, i), m2.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Divide(f.apply(m1), f.apply(m2));
        }
    }

    class Modulus extends Binop {
        public Modulus(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Modulus(m1.transform(f, g, h, i), m2.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Modulus(f.apply(m1), f.apply(m2));
        }
    }

    class Power extends Binop {
        public Power(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Power(m1.transform(f, g, h, i), m2.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Power(f.apply(m1), f.apply(m2));
        }
    }

    class Parent implements AggregateMetric {
        public final AggregateMetric metric;

        public Parent(AggregateMetric metric) {
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Parent(metric.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Parent(f.apply(metric));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Parent parent = (Parent) o;
            return Objects.equals(metric, parent.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metric);
        }
    }

    class Lag implements AggregateMetric {
        public final int lag;
        public final AggregateMetric metric;

        public Lag(int lag, AggregateMetric metric) {
            this.lag = lag;
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Lag(lag, metric.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Lag(lag, f.apply(metric));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Lag lag1 = (Lag) o;
            return Objects.equals(lag, lag1.lag) &&
                    Objects.equals(metric, lag1.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lag, metric);
        }
    }

    class Window implements AggregateMetric {
        public final int window;
        public final AggregateMetric metric;

        public Window(int window, AggregateMetric metric) {
            this.window = window;
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Window(window, metric.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Window(window, f.apply(metric));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Window window1 = (Window) o;
            return Objects.equals(window, window1.window) &&
                    Objects.equals(metric, window1.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(window, metric);
        }
    }

    class Qualified implements AggregateMetric {
        public final List<String> scope;
        public final AggregateMetric metric;

        public Qualified(List<String> scope, AggregateMetric metric) {
            this.scope = scope;
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Qualified(scope, metric.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Qualified(scope, f.apply(metric));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Qualified qualified = (Qualified) o;
            return Objects.equals(scope, qualified.scope) &&
                    Objects.equals(metric, qualified.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scope, metric);
        }
    }

    class DocStatsPushes implements AggregateMetric {
        public final String dataset;
        public final List<String> pushes;

        public DocStatsPushes(String dataset, List<String> pushes) {
            this.dataset = dataset;
            this.pushes = pushes;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DocStatsPushes that = (DocStatsPushes) o;
            return Objects.equals(dataset, that.dataset) &&
                    Objects.equals(pushes, that.pushes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataset, pushes);
        }
    }

    class DocStats implements AggregateMetric {
        public final DocMetric metric;

        public DocStats(DocMetric metric) {
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new DocStats(metric.transform(g, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DocStats docStats = (DocStats) o;
            return Objects.equals(metric, docStats.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metric);
        }
    }

    /**
     * DocStats in which there is no explicit sum, but a single atomic token. Could be a boolean on DocStats but whatever.
     */
    class ImplicitDocStats implements AggregateMetric {
        public final String field;

        public ImplicitDocStats(String field) {
            this.field = field;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ImplicitDocStats that = (ImplicitDocStats) o;
            return Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field);
        }
    }

    class Constant implements AggregateMetric {
        public final double value;

        public Constant(double value) {
            this.value = value;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Constant constant = (Constant) o;
            return Objects.equals(value, constant.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    class Percentile implements AggregateMetric {
        public final String field;
        public final double percentile;

        public Percentile(String field, double percentile) {
            this.field = field;
            this.percentile = percentile;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Percentile that = (Percentile) o;
            return Objects.equals(percentile, that.percentile) &&
                    Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, percentile);
        }
    }

    class Running implements AggregateMetric {
        public final AggregateMetric metric;

        public Running(AggregateMetric metric) {
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Running(metric.transform(f, g, h, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Running(f.apply(metric));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Running running = (Running) o;
            return Objects.equals(metric, running.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metric);
        }
    }

    class Distinct implements AggregateMetric {
        public final String field;
        public final Optional<AggregateFilter> filter;
        public final Optional<Integer> windowSize;

        public Distinct(String field, Optional<AggregateFilter> filter, Optional<Integer> windowSize) {
            this.field = field;
            this.filter = filter;
            this.windowSize = windowSize;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            if (filter.isPresent()) {
                return f.apply(new Distinct(field, Optional.of(filter.get().transform(f, g, h, i)), windowSize));
            } else {
                return f.apply(this);
            }
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            if (filter.isPresent()) {
                return new Distinct(field, Optional.of(filter.get().traverse1(f)), windowSize);
            } else {
                return this;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Distinct distinct = (Distinct) o;
            return Objects.equals(field, distinct.field) &&
                    Objects.equals(filter, distinct.filter) &&
                    Objects.equals(windowSize, distinct.windowSize);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, filter, windowSize);
        }
    }

    class Named implements AggregateMetric {
        public final AggregateMetric metric;
        public final String name;

        public Named(AggregateMetric metric, String name) {
            this.metric = metric;
            this.name = name;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return f.apply(new Named(metric.transform(f, g, h, i), name));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Named(f.apply(metric), name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Named named = (Named) o;
            return Objects.equals(metric, named.metric) &&
                    Objects.equals(name, named.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metric, name);
        }
    }
}
