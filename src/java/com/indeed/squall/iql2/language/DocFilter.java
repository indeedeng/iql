package com.indeed.squall.iql2.language;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.indeed.flamdex.lucene.LuceneQueryTranslator;
import com.indeed.flamdex.query.BooleanOp;
import com.indeed.squall.iql2.language.actions.Action;
import com.indeed.squall.iql2.language.actions.IntOrAction;
import com.indeed.squall.iql2.language.actions.MetricAction;
import com.indeed.squall.iql2.language.actions.QueryAction;
import com.indeed.squall.iql2.language.actions.RegexAction;
import com.indeed.squall.iql2.language.actions.SampleAction;
import com.indeed.squall.iql2.language.actions.StringOrAction;
import com.indeed.squall.iql2.language.actions.UnconditionalAction;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import com.indeed.squall.iql2.language.util.ErrorMessages;
import com.indeed.squall.iql2.language.util.MapUtil;
import com.indeed.squall.iql2.language.util.ValidationUtil;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public interface DocFilter {

    DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i);

    DocMetric asZeroOneMetric(String dataset);

    List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier);

    void validate(String dataset, DatasetsFields datasetsFields, Validator validator);

    class FieldIs implements DocFilter {
        public final Map<String, Set<String>> datasetToKeywordAnalyzerFields;
        public final String field;
        public final Term term;

        public FieldIs(Map<String, Set<String>> datasetToKeywordAnalyzerFields, String field, Term term) {
            // TODO: Immutable clone
            this.datasetToKeywordAnalyzerFields = datasetToKeywordAnalyzerFields;
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
                final List<String> tokens = tokenize(dataset);
                if (tokens.size() == 1) {
                    return new DocMetric.HasString(field, term.stringTerm);
                }
                DocMetric docMetric = null;
                for (final String token : tokens) {
                    if (docMetric == null) {
                        docMetric = new DocMetric.HasString(field, token);
                    } else {
                        docMetric = new DocMetric.Add(docMetric, new DocMetric.HasString(field, token));
                    }
                }
                return new DocMetric.MetricEqual(docMetric, new DocMetric.Constant(tokens.size()));
            }
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            final Map<String, com.indeed.flamdex.query.Query> datasetToQuery = new HashMap<>();
            if (term.isIntTerm) {
                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newTermQuery(term.toFlamdex(field));
                for (final String dataset : scope.keySet()) {
                    datasetToQuery.put(dataset, query);
                }
            } else {
                for (final String dataset : scope.keySet()) {
                    final List<String> tokens = tokenize(scope.get(dataset));
                    final List<com.indeed.flamdex.query.Query> clauses = new ArrayList<>();
                    for (final String token : tokens) {
                        clauses.add(com.indeed.flamdex.query.Query.newTermQuery(com.indeed.flamdex.query.Term.stringTerm(field, token)));
                    }
                    final com.indeed.flamdex.query.Query query;
                    if (clauses.size() == 1) {
                        query = clauses.get(0);
                    } else {
                        query = com.indeed.flamdex.query.Query.newBooleanQuery(BooleanOp.AND, clauses);
                    }
                    datasetToQuery.put(dataset, query);
                }
            }
            return Collections.<Action>singletonList(new QueryAction(scope.keySet(), datasetToQuery, target, positive, negative));
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            if (term.isIntTerm) {
                if (!datasetsFields.getIntFields(dataset).contains(field)) {
                    validator.error(ErrorMessages.missingIntField(dataset, field, this));
                }
            } else {
                if (!datasetsFields.getStringFields(dataset).contains(field)) {
                    validator.error(ErrorMessages.missingStringField(dataset, field, this));
                }
            }
        }

        private List<String> tokenize(String dataset) {
            if (term.isIntTerm) {
                throw new IllegalStateException("Called tokenize on int term?!");
            }
            final Set<String> keywordAnalyzerFields;
            if (datasetToKeywordAnalyzerFields.containsKey(dataset)) {
                keywordAnalyzerFields = datasetToKeywordAnalyzerFields.get(dataset);
            } else {
                keywordAnalyzerFields = Collections.emptySet();
            }
            if (keywordAnalyzerFields.contains(field) || keywordAnalyzerFields.contains("*")) {
                return Collections.singletonList(term.stringTerm);
            }
            final List<String> tokens = new ArrayList<>();
            final WhitespaceAnalyzer whitespaceAnalyzer = new WhitespaceAnalyzer();
            try {
                final TokenStream tokenStream = whitespaceAnalyzer.tokenStream(null, new StringReader(term.stringTerm));
                final Token token = new Token();
                try {
                    while (tokenStream.next(token) != null) {
                        tokens.add(token.term());
                    }
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            } finally {
                whitespaceAnalyzer.close();
            }
            if (tokens.isEmpty()) {
                throw new IllegalStateException("Found no terms in WhitespaceAnalyzer field: " + field + ", term = [" + term + "]");
            }
            return tokens;
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
        public final Map<String, Set<String>> datasetToKeywordAnalyzerFields;

        public FieldIsnt(Map<String, Set<String>> datasetToKeywordAnalyzerFields, String field, Term term) {
            this.datasetToKeywordAnalyzerFields = datasetToKeywordAnalyzerFields;
            this.field = field;
            this.term = term;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new Not(new FieldIs(datasetToKeywordAnalyzerFields, field, term)).asZeroOneMetric(dataset);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return new Not(new FieldIs(datasetToKeywordAnalyzerFields, field, term)).getExecutionActions(scope, target, positive, negative, groupSupplier);
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            if (term.isIntTerm) {
                if (!datasetsFields.getIntFields(dataset).contains(field)) {
                    validator.error(ErrorMessages.missingIntField(dataset, field, this));
                }
            } else {
                if (!datasetsFields.getStringFields(dataset).contains(field)) {
                    validator.error(ErrorMessages.missingStringField(dataset, field, this));
                }
            }
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

    class FieldInQuery implements DocFilter {
        public final com.indeed.squall.iql2.language.query.Query query;
        public final ScopedField field;
        public final boolean isNegated; // true if <field> NOT IN <query>

        public FieldInQuery(com.indeed.squall.iql2.language.query.Query query, ScopedField field, boolean isNegated) {
            this.query = query;
            this.field = field;
            this.isNegated = isNegated;
        }


        // TODO: Should this propagate the transformation or just allow `i` to recurse if desired?
        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            throw new UnsupportedOperationException("Must transform the FieldInQuery out before doing a .asZeroOneMetric()");
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            throw new UnsupportedOperationException("Must transform the FieldInQuery out before doing a .getExecutionActions()");
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            // TODO: Should we do anything here? I don't think so...
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FieldInQuery that = (FieldInQuery) o;

            if (isNegated != that.isNegated) return false;
            if (query != null ? !query.equals(that.query) : that.query != null) return false;
            return !(field != null ? !field.equals(that.field) : that.field != null);

        }

        @Override
        public int hashCode() {
            int result = query != null ? query.hashCode() : 0;
            result = 31 * result + (field != null ? field.hashCode() : 0);
            result = 31 * result + (isNegated ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "FieldInQuery{" +
                    "query=" + query +
                    ", field='" + field + '\'' +
                    ", isNegated=" + isNegated +
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, lowerBound, upperBound, false);
            return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            if (!datasetsFields.getIntFields(dataset).contains(field)) {
                validator.error(ErrorMessages.missingIntField(dataset, field, this));
            }
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
//            if (m1 instanceof DocMetric.Field && m2 instanceof DocMetric.Constant) {
//                final String field = ((DocMetric.Field) m1).field;
//                final long value = ((DocMetric.Constant) m2).value;
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newTermQuery(com.indeed.flamdex.query.Term.intTerm(field, value));
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else if (m1 instanceof DocMetric.Constant && m2 instanceof DocMetric.Field) {
//                final String field = ((DocMetric.Field) m2).field;
//                final long value = ((DocMetric.Constant) m1).value;
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newTermQuery(com.indeed.flamdex.query.Term.intTerm(field, value));
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else {
                return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
//            }
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            m1.validate(dataset, datasetsFields, validator);
            m2.validate(dataset, datasetsFields, validator);
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            // TODO: Not duplicate logic across these two branches
//            if (m1 instanceof DocMetric.Field && m2 instanceof DocMetric.Constant) {
//                final String field = ((DocMetric.Field) m1).field;
//                final long value = ((DocMetric.Constant) m2).value;
//                return getFieldNotEqualValue(scope, field, value, target, positive, negative);
//            } else if (m1 instanceof DocMetric.Constant && m2 instanceof DocMetric.Field) {
//                final String field = ((DocMetric.Field) m2).field;
//                final long value = ((DocMetric.Constant) m1).value;
//                return getFieldNotEqualValue(scope, field, value, target, positive, negative);
//            } else {
                return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
//            }
        }

        private static List<Action> getFieldNotEqualValue(Map<String, String> scope, String field, long value, int target, int positive, int negative) {
            final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newTermQuery(com.indeed.flamdex.query.Term.intTerm(field, value));
            final com.indeed.flamdex.query.Query negated = com.indeed.flamdex.query.Query.newBooleanQuery(BooleanOp.NOT, Collections.singletonList(query));
            return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, negated), target, positive, negative));
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            m1.validate(dataset, datasetsFields, validator);
            m2.validate(dataset, datasetsFields, validator);
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
//            if (m1 instanceof DocMetric.Field && m2 instanceof DocMetric.Constant) {
//                final String field = ((DocMetric.Field) m1).field;
//                final long value = ((DocMetric.Constant) m2).value;
//                // field > value => field in [value + 1, MAX_VALUE]
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, value + 1, Long.MAX_VALUE, true);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else if (m1 instanceof DocMetric.Constant && m2 instanceof DocMetric.Field) {
//                final String field = ((DocMetric.Field) m2).field;
//                final long value = ((DocMetric.Constant) m1).value;
//                // value > field => field in [MIN_VALUE, value)
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, Long.MIN_VALUE, value, false);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else {
                return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
//            }
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            m1.validate(dataset, datasetsFields, validator);
            m2.validate(dataset, datasetsFields, validator);
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
//            if (m1 instanceof DocMetric.Field && m2 instanceof DocMetric.Constant) {
//                final String field = ((DocMetric.Field) m1).field;
//                final long value = ((DocMetric.Constant) m2).value;
//                // field >= value => field in [value, MAX_VALUE]
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, value, Long.MAX_VALUE, true);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else if (m1 instanceof DocMetric.Constant && m2 instanceof DocMetric.Field) {
//                final String field = ((DocMetric.Field) m2).field;
//                final long value = ((DocMetric.Constant) m1).value;
//                // value >= field => field in [MIN_VALUE, value]
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, Long.MIN_VALUE, value, true);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else {
                return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
//            }
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            m1.validate(dataset, datasetsFields, validator);
            m2.validate(dataset, datasetsFields, validator);
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
//            if (m1 instanceof DocMetric.Field && m2 instanceof DocMetric.Constant) {
//                final String field = ((DocMetric.Field) m1).field;
//                final long value = ((DocMetric.Constant) m2).value;
//                // field < value => field in [MIN_VALUE, value)
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, Long.MIN_VALUE, value, false);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else if (m1 instanceof DocMetric.Constant && m2 instanceof DocMetric.Field) {
//                final String field = ((DocMetric.Field) m2).field;
//                final long value = ((DocMetric.Constant) m1).value;
//                // value < field => field in [value + 1, MAX_VALUE]
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, value + 1, Long.MAX_VALUE, true);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else {
                return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
//            }
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            m1.validate(dataset, datasetsFields, validator);
            m2.validate(dataset, datasetsFields, validator);
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
//            if (m1 instanceof DocMetric.Field && m2 instanceof DocMetric.Constant) {
//                final String field = ((DocMetric.Field) m1).field;
//                final long value = ((DocMetric.Constant) m2).value;
//                // field <= value => field in [MIN_VALUE, value]
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, Long.MIN_VALUE, value, true);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else if (m1 instanceof DocMetric.Constant && m2 instanceof DocMetric.Field) {
//                final String field = ((DocMetric.Field) m2).field;
//                final long value = ((DocMetric.Constant) m1).value;
//                // value <= field => field in [value, MAX_VALUE]
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, value, Long.MAX_VALUE, true);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else {
                return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
//            }
        }

        @Override
        public void validate(final String dataset, final DatasetsFields datasetsFields, final Validator validator) {
            m1.validate(dataset, datasetsFields, validator);
            m2.validate(dataset, datasetsFields, validator);
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            final List<Action> result = new ArrayList<>();
            if (target != negative && positive != negative) {
                result.addAll(f1.getExecutionActions(scope, target, target, negative, groupSupplier));
                result.addAll(f2.getExecutionActions(scope, target, positive, negative, groupSupplier));
            } else {
                final int newGroup = groupSupplier.acquire();
                result.addAll(f1.getExecutionActions(scope, target, target, newGroup, groupSupplier));
                result.addAll(f2.getExecutionActions(scope, target, target, newGroup, groupSupplier));
                result.add(new UnconditionalAction(scope.keySet(), target, positive));
                result.add(new UnconditionalAction(scope.keySet(), newGroup, target));
                groupSupplier.release(newGroup);
            }
            return result;
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            f1.validate(dataset, datasetsFields, validator);
            f2.validate(dataset, datasetsFields, validator);
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            final List<Action> result = new ArrayList<>();
            if (target != positive && positive != negative) {
                result.addAll(f1.getExecutionActions(scope, target, positive, target, groupSupplier));
                result.addAll(f2.getExecutionActions(scope, target, positive, negative, groupSupplier));
            } else {
                final int newGroup = groupSupplier.acquire();
                result.addAll(f1.getExecutionActions(scope, target, newGroup, target, groupSupplier));
                result.addAll(f2.getExecutionActions(scope, target, newGroup, target, groupSupplier));
                result.add(new UnconditionalAction(scope.keySet(), target, negative));
                result.add(new UnconditionalAction(scope.keySet(), newGroup, positive));
                groupSupplier.release(newGroup);
            }
            return result;
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            f1.validate(dataset, datasetsFields, validator);
            f2.validate(dataset, datasetsFields, validator);
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
            if (filters.isEmpty()) {
                return new DocMetric.Constant(0);
            }
            DocMetric metric = filters.get(0).asZeroOneMetric(dataset);
            for (int i = 1; i < filters.size(); i++) {
                metric = new DocMetric.Add(metric, filters.get(i).asZeroOneMetric(dataset));
            }
            return new DocMetric.MetricGt(metric, new DocMetric.Constant(0));
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            if (filters.isEmpty()) {
                return Collections.<Action>singletonList(new UnconditionalAction(scope.keySet(), target, negative));
            }
            DocFilter reOrred = filters.get(0);
            for (int i = 1; i < filters.size(); i++) {
                reOrred = new Or(filters.get(i), reOrred);
            }
            return reOrred.getExecutionActions(scope, target, positive, negative, groupSupplier);
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            for (final DocFilter filter : filters) {
                filter.validate(dataset, datasetsFields, validator);
            }
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return filter.getExecutionActions(scope, target, negative, positive, groupSupplier);
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            filter.validate(dataset, datasetsFields, validator);
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.<Action>singletonList(new RegexAction(scope.keySet(), field, regex, target, positive, negative));
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            if (!datasetsFields.getStringFields(dataset).contains(field)) {
                validator.error(ErrorMessages.missingStringField(dataset, field, this));
            }
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return new Not(new Regex(field, regex)).getExecutionActions(scope, target, positive, negative, groupSupplier);
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            if (!datasetsFields.getStringFields(dataset).contains(field)) {
                validator.error(ErrorMessages.missingStringField(dataset, field, this));
            }
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
        // TODO: Why is this a List<String> instead of a single string? A per-document thing can only be one dataset!
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            final Map<String, String> restrictedScope = new HashMap<>();
            for (final String dataset : this.scope) {
                restrictedScope.put(dataset, scope.get(dataset));
            }
            return filter.getExecutionActions(restrictedScope, target, positive, negative, groupSupplier);
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            if (scope.contains(dataset)) {
                filter.validate(dataset, datasetsFields, validator);
            }
            ValidationUtil.validateScope(scope, datasetsFields, validator);
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
        // TODO: Ensure capitalization
        private final Map<String, Set<String>> datasetToKeywordAnalyzerFields;
        private final Map<String, Set<String>> datasetToIntFields;

        public Lucene(String query, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields) {
            this.query = query;
            this.datasetToKeywordAnalyzerFields = datasetToKeywordAnalyzerFields;
            this.datasetToIntFields = datasetToIntFields;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            final com.indeed.flamdex.query.Query rewritten = getFlamdexQuery(dataset);
            final DocFilter filter = FlamdexQueryTranslator.translate(rewritten, datasetToKeywordAnalyzerFields);
            return filter.asZeroOneMetric(dataset);
        }

        private com.indeed.flamdex.query.Query getFlamdexQuery(String dataset) {
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
            // TODO: Uppercase all of the fields.
            return LuceneQueryTranslator.rewrite(parsed, datasetToIntFields.containsKey(dataset) ? datasetToIntFields.get(dataset) : Collections.<String>emptySet());
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            final Map<String, com.indeed.flamdex.query.Query> datasetToQuery = new HashMap<>();
            for (final String dataset : scope.keySet()) {
                datasetToQuery.put(dataset, getFlamdexQuery(scope.get(dataset)));
            }
            return Collections.<Action>singletonList(new QueryAction(scope.keySet(), datasetToQuery, target, positive, negative));
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            ValidationUtil.ensureSubset(datasetsFields, ValidationUtil.findFieldsUsed(ImmutableMap.of(dataset, getFlamdexQuery(dataset))), validator, this, true);
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.<Action>singletonList(new SampleAction(scope.keySet(), field, (double) numerator / denominator, seed, target, positive, negative));
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            if (!datasetsFields.getAllFields(dataset).contains(field)) {
                validator.error(ErrorMessages.missingField(dataset, field, this));
            }
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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.<Action>singletonList(new UnconditionalAction(scope.keySet(), target, positive));
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {

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
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.<Action>singletonList(new UnconditionalAction(scope.keySet(), target, negative));
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {

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

    class StringFieldIn implements DocFilter {
        public final Map<String, Set<String>> datasetToKeywordAnalyzerFields;
        public final String field;
        public final Set<String> terms;

        public StringFieldIn(Map<String, Set<String>> datasetToKeywordAnalyzerFields, String field, Set<String> terms) {
            this.datasetToKeywordAnalyzerFields = datasetToKeywordAnalyzerFields;
            if (terms.isEmpty()) {
                throw new IllegalArgumentException("Cannot have empty set of terms!");
            }
            this.field = field;
            this.terms = ImmutableSet.copyOf(terms);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            DocFilter filter = null;
            for (final String term : terms) {
                if (filter == null) {
                    filter = new FieldIs(datasetToKeywordAnalyzerFields, field, Term.term(term));
                } else {
                    filter = new Or(filter, new FieldIs(datasetToKeywordAnalyzerFields, field, Term.term(term)));
                }
            }
            return filter.asZeroOneMetric(dataset);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            // TODO: Should this care about the keyword analyzer fields?
            return Collections.<Action>singletonList(new StringOrAction(scope.keySet(), field, terms, target, positive, negative));
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            if (!datasetsFields.getStringFields(dataset).contains(field)) {
                validator.error(ErrorMessages.missingStringField(dataset, field, this));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StringFieldIn that = (StringFieldIn) o;
            return Objects.equals(field, that.field) &&
                    Objects.equals(terms, that.terms);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, terms);
        }

        @Override
        public String toString() {
            return "StringFieldIn{" +
                    "field='" + field + '\'' +
                    ", terms=" + terms +
                    '}';
        }
    }

    class IntFieldIn implements DocFilter {
        public final String field;
        public final Set<Long> terms;

        public IntFieldIn(String field, Set<Long> terms) {
            if (terms.isEmpty()) {
                throw new IllegalArgumentException("Cannot have empty set of terms!");
            }
            this.field = field;
            this.terms = new LongOpenHashSet(terms);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            DocFilter filter = null;
            for (final long term : terms) {
                if (filter == null) {
                    filter = new FieldIs(Collections.<String, Set<String>>emptyMap(), field, Term.term(term));
                } else {
                    filter = new Or(filter, new FieldIs(Collections.<String, Set<String>>emptyMap(), field, Term.term(term)));
                }
            }
            return filter.asZeroOneMetric(dataset);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.<Action>singletonList(new IntOrAction(scope.keySet(), field, terms, target, positive, negative));
        }

        @Override
        public void validate(String dataset, DatasetsFields datasetsFields, Validator validator) {
            if (!datasetsFields.getAllFields(dataset).contains(field)) {
                validator.error(ErrorMessages.missingField(dataset, field, this));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IntFieldIn that = (IntFieldIn) o;
            return Objects.equals(field, that.field) &&
                    Objects.equals(terms, that.terms);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, terms);
        }

        @Override
        public String toString() {
            return "IntFieldIn{" +
                    "field='" + field + '\'' +
                    ", terms=" + terms +
                    '}';
        }
    }
}
