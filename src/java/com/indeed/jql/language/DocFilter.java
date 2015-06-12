package com.indeed.jql.language;

import com.google.common.base.Function;
import com.indeed.flamdex.lucene.LuceneQueryTranslator;
import com.indeed.flamdex.query.BooleanOp;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public interface DocFilter {

    DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i);

    DocMetric asZeroOneMetric(String dataset);

    class FieldIs implements DocFilter {
        public final String field;
        public final Term term;

        public FieldIs(String field, Term term) {
            this.field = field;
            this.term = term;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            if (term.isIntTerm) {
                return new DocMetric.HasInt(field, term.intTerm);
            } else {
                return new DocMetric.HasString(field, term.stringTerm);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FieldIs fieldIs = (FieldIs) o;
            return Objects.equals(field, fieldIs.field) &&
                    Objects.equals(term, fieldIs.term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, term);
        }

        @Override
        public String toString() {
            return "FieldIs{" +
                    "field='" + field + '\'' +
                    ", term=" + term +
                    '}';
        }
    }

    class FieldIsnt implements DocFilter {
        public final String field;
        public final Term term;

        public FieldIsnt(String field, Term term) {
            this.field = field;
            this.term = term;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new Not(new FieldIs(field, term)).asZeroOneMetric(dataset);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FieldIsnt fieldIsnt = (FieldIsnt) o;
            return Objects.equals(field, fieldIsnt.field) &&
                    Objects.equals(term, fieldIsnt.term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, term);
        }

        @Override
        public String toString() {
            return "FieldIsnt{" +
                    "field='" + field + '\'' +
                    ", term=" + term +
                    '}';
        }
    }

    class Between implements DocFilter {
        public final String field;
        public final long lowerBound;
        public final long upperBound;

        public Between(String field, long lowerBound, long upperBound) {
            this.field = field;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new And(
                new MetricGte(new DocMetric.Field(field), new DocMetric.Constant(lowerBound)),
                new MetricLt(new DocMetric.Field(field), new DocMetric.Constant(upperBound))
            ).asZeroOneMetric(dataset);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Between between = (Between) o;
            return Objects.equals(lowerBound, between.lowerBound) &&
                    Objects.equals(upperBound, between.upperBound) &&
                    Objects.equals(field, between.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, lowerBound, upperBound);
        }

        @Override
        public String toString() {
            return "Between{" +
                    "field='" + field + '\'' +
                    ", lowerBound=" + lowerBound +
                    ", upperBound=" + upperBound +
                    '}';
        }
    }

    class MetricEqual implements DocFilter {
        public final DocMetric m1;
        public final DocMetric m2;

        public MetricEqual(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricEqual(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricEqual(m1, m2);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricEqual that = (MetricEqual) o;
            return Objects.equals(m1, that.m1) &&
                    Objects.equals(m2, that.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricEqual{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    class MetricNotEqual implements DocFilter {
        public final DocMetric m1;
        public final DocMetric m2;

        public MetricNotEqual(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricNotEqual(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricNotEqual(m1, m2);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricNotEqual that = (MetricNotEqual) o;
            return Objects.equals(m1, that.m1) &&
                    Objects.equals(m2, that.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricNotEqual{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    class MetricGt implements DocFilter {
        public final DocMetric m1;
        public final DocMetric m2;

        public MetricGt(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricGt(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricGt(m1, m2);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricGt metricGt = (MetricGt) o;
            return Objects.equals(m1, metricGt.m1) &&
                    Objects.equals(m2, metricGt.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricGt{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    class MetricGte implements DocFilter {
        public final DocMetric m1;
        public final DocMetric m2;

        public MetricGte(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricGte(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricGte(m1, m2);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricGte metricGte = (MetricGte) o;
            return Objects.equals(m1, metricGte.m1) &&
                    Objects.equals(m2, metricGte.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricGte{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    class MetricLt implements DocFilter {
        public final DocMetric m1;
        public final DocMetric m2;

        public MetricLt(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricLt(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricLt(m1, m2);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricLt metricLt = (MetricLt) o;
            return Objects.equals(m1, metricLt.m1) &&
                    Objects.equals(m2, metricLt.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricLt{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    class MetricLte implements DocFilter {
        public final DocMetric m1;
        public final DocMetric m2;

        public MetricLte(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricLte(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricLte(m1, m2);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricLte metricLte = (MetricLte) o;
            return Objects.equals(m1, metricLte.m1) &&
                    Objects.equals(m2, metricLte.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricLte{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    class And implements DocFilter {
        public final DocFilter f1;
        public final DocFilter f2;

        public And(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new And(f1.transform(g, i), f2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricEqual(new DocMetric.Add(f1.asZeroOneMetric(dataset), f2.asZeroOneMetric(dataset)), new DocMetric.Constant(2));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            And and = (And) o;
            return Objects.equals(f1, and.f1) &&
                    Objects.equals(f2, and.f2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(f1, f2);
        }

        @Override
        public String toString() {
            return "And{" +
                    "f1=" + f1 +
                    ", f2=" + f2 +
                    '}';
        }
    }

    class Or implements DocFilter {
        public final DocFilter f1;
        public final DocFilter f2;

        public Or(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Or(f1.transform(g, i), f2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricGt(new DocMetric.Add(f1.asZeroOneMetric(dataset), f2.asZeroOneMetric(dataset)), new DocMetric.Constant(0));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Or or = (Or) o;
            return Objects.equals(f1, or.f1) &&
                    Objects.equals(f2, or.f2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(f1, f2);
        }

        @Override
        public String toString() {
            return "Or{" +
                    "f1=" + f1 +
                    ", f2=" + f2 +
                    '}';
        }
    }

    class Ors implements DocFilter {
        public final List<DocFilter> filters;

        public Ors(List<DocFilter> filters) {
            if (filters.size() < 2) {
                throw new IllegalArgumentException("Can't OR a single thing.");
            }
            this.filters = filters;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            final List<DocFilter> transformed = new ArrayList<>();
            for (final DocFilter filter : filters) {
                transformed.add(filter.transform(g, i));
            }
            return i.apply(new Ors(transformed));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            DocMetric metric = filters.get(0).asZeroOneMetric(dataset);
            for (int i = 1; i < filters.size(); i++) {
                metric = new DocMetric.Add(metric, filters.get(i).asZeroOneMetric(dataset));
            }
            return new DocMetric.MetricGt(metric, new DocMetric.Constant(0));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Ors ors = (Ors) o;
            return Objects.equals(filters, ors.filters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filters);
        }


        @Override
        public String toString() {
            return "Ors{" +
                    "filters=" + filters +
                    '}';
        }
    }

    class Not implements DocFilter {
        public final DocFilter filter;

        public Not(DocFilter filter) {
            this.filter = filter;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Not(filter.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.Subtract(new DocMetric.Constant(1), filter.asZeroOneMetric(dataset));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Not not = (Not) o;
            return Objects.equals(filter, not.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filter);
        }

        @Override
        public String toString() {
            return "Not{" +
                    "filter=" + filter +
                    '}';
        }
    }

    class Regex implements DocFilter {
        public final String field;
        public final String regex;

        public Regex(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.RegexMetric(field, regex);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Regex regex1 = (Regex) o;
            return Objects.equals(field, regex1.field) &&
                    Objects.equals(regex, regex1.regex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, regex);
        }

        @Override
        public String toString() {
            return "Regex{" +
                    "field='" + field + '\'' +
                    ", regex='" + regex + '\'' +
                    '}';
        }
    }

    class NotRegex implements DocFilter {
        public final String field;
        public final String regex;

        public NotRegex(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new Not(new Regex(field, regex)).asZeroOneMetric(dataset);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NotRegex notRegex = (NotRegex) o;
            return Objects.equals(field, notRegex.field) &&
                    Objects.equals(regex, notRegex.regex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, regex);
        }

        @Override
        public String toString() {
            return "NotRegex{" +
                    "field='" + field + '\'' +
                    ", regex='" + regex + '\'' +
                    '}';
        }
    }

    class Qualified implements DocFilter {
        public final List<String> scope;
        public final DocFilter filter;

        public Qualified(List<String> scope, DocFilter filter) {
            this.scope = scope;
            this.filter = filter;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Qualified(scope, filter.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            if (scope.contains(dataset)) {
                return filter.asZeroOneMetric(dataset);
            } else {
                // TODO: Is this what we want to be doing?
                return new DocMetric.Constant(1);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Qualified qualified = (Qualified) o;
            return Objects.equals(scope, qualified.scope) &&
                    Objects.equals(filter, qualified.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scope, filter);
        }

        @Override
        public String toString() {
            return "Qualified{" +
                    "scope=" + scope +
                    ", filter=" + filter +
                    '}';
        }
    }

    class Lucene implements DocFilter {
        public final String query;
        private final Map<String, Set<String>> datasetToKeywordAnalyzerFields;

        public Lucene(String query, Map<String, Set<String>> datasetToKeywordAnalyzerFields) {
            this.query = query;
            this.datasetToKeywordAnalyzerFields = datasetToKeywordAnalyzerFields;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            final Analyzer analyzer;
            // TODO: Detect if imhotep index and use KeywordAnalyzer always in that case..?
            if (datasetToKeywordAnalyzerFields.containsKey(dataset)) {
                final KeywordAnalyzer kwAnalyzer = new KeywordAnalyzer();
                final Set<String> whitelist = datasetToKeywordAnalyzerFields.get(dataset);
                if (whitelist.contains("*")) {
                    analyzer = kwAnalyzer;
                } else {
                    final PerFieldAnalyzerWrapper perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer());
                    for (final String field : whitelist) {
                        perFieldAnalyzerWrapper.addAnalyzer(field, kwAnalyzer);
                    }
                    analyzer = perFieldAnalyzerWrapper;
                }
            } else {
                analyzer = new WhitespaceAnalyzer();
            }
            final QueryParser qp = new QueryParser("foo", analyzer);
            qp.setDefaultOperator(QueryParser.Operator.AND);
            final Query parsed;
            try {
                parsed = qp.parse(query);
            } catch (ParseException e) {
                throw new IllegalArgumentException("Could not parse lucene term: " + query, e);
            }
            // TODO: Get int fields?
            final com.indeed.flamdex.query.Query rewritten = LuceneQueryTranslator.rewrite(parsed, Collections.<String>emptySet());
            final DocFilter filter = FlamdexQueryTranslator.translate(rewritten);
            return filter.asZeroOneMetric(dataset);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Lucene lucene = (Lucene) o;
            return Objects.equals(query, lucene.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query);
        }

        @Override
        public String toString() {
            return "Lucene{" +
                    "query='" + query + '\'' +
                    '}';
        }
    }

    class Sample implements DocFilter {
        public final String field;
        public final long numerator;
        public final long denominator;
        public final String seed;

        public Sample(String field, long numerator, long denominator, String seed) {
            this.field = field;
            this.numerator = numerator;
            this.denominator = denominator;
            this.seed = seed;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            // TODO: Support this
            throw new UnsupportedOperationException("Haven't implemented Sample yet");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Sample sample = (Sample) o;
            return Objects.equals(numerator, sample.numerator) &&
                    Objects.equals(denominator, sample.denominator) &&
                    Objects.equals(field, sample.field) &&
                    Objects.equals(seed, sample.seed);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, numerator, denominator, seed);
        }

        @Override
        public String toString() {
            return "Sample{" +
                    "field='" + field + '\'' +
                    ", numerator=" + numerator +
                    ", denominator=" + denominator +
                    ", seed='" + seed + '\'' +
                    '}';
        }
    }

    class Always implements DocFilter {
        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.Constant(1);
        }

        @Override
        public int hashCode() {
            return 1;
        }

        @Override
        public boolean equals(Object obj) {
            return getClass().equals(obj.getClass());
        }

        @Override
        public String toString() {
            return "Always{}";
        }
    }

    class Never implements DocFilter {
        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.Constant(0);
        }

        @Override
        public int hashCode() {
            return 2;
        }

        @Override
        public boolean equals(Object obj) {
            return getClass().equals(obj.getClass());
        }

        @Override
        public String toString() {
            return "Never{}";
        }
    }
}
