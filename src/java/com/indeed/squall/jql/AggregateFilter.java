package com.indeed.squall.jql;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;
import com.indeed.squall.jql.metrics.aggregate.AggregateMetric;
import com.indeed.squall.jql.metrics.aggregate.PerGroupConstant;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * @author jwolfe
 */
public interface AggregateFilter extends Pushable{
    boolean[] getGroupStats(long[][] stats, int numGroups);

    boolean allow(String term, long[] stats, int group);
    boolean allow(long term, long[] stats, int group);

    static AggregateFilter fromJson(JsonNode node, Function<String, PerGroupConstant> namedMetricLookup) {
        final Supplier<AggregateMetric> m1 = () -> AggregateMetric.fromJson(node.get("arg1"), namedMetricLookup);
        final Supplier<AggregateMetric> m2 = () -> AggregateMetric.fromJson(node.get("arg2"), namedMetricLookup);
        final Supplier<AggregateFilter> f1 = () -> AggregateFilter.fromJson(node.get("arg1"), namedMetricLookup);
        final Supplier<AggregateFilter> f2 = () -> AggregateFilter.fromJson(node.get("arg2"), namedMetricLookup);
        switch (node.get("type").textValue()) {
            case "termEquals":
                return new TermEquals(Term.fromJson(node.get("value")));
            case "not":
                return new Not(AggregateFilter.fromJson(node.get("value"), namedMetricLookup));
            case "regex":
                return new RegexFilter(node.get("field").textValue(), node.get("value").textValue());
            case "metricEquals":
                return new MetricEquals(m1.get(), m2.get());
            case "metricNotEquals":
                return new MetricNotEquals(m1.get(), m2.get());
            case "greaterThan":
                return new GreaterThan(m1.get(), m2.get());
            case "greaterThanOrEquals":
                return new GreaterThanOrEquals(m1.get(), m2.get());
            case "lessThan":
                return new LessThan(m1.get(), m2.get());
            case "lessThanOrEquals":
                return new LessThanOrEquals(m1.get(), m2.get());
            case "and":
                return new And(f1.get(), f2.get());
            case "or":
                return new Or(f1.get(), f2.get());
            case "always":
                return new Constant(true);
        }
        throw new RuntimeException("Oops: " + node);
    }

    class TermEquals implements AggregateFilter {
        private final Term value;

        public TermEquals(Term value) {
            this.value = value;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Collections.emptySet();
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
        }

        @Override
        public boolean[] getGroupStats(long[][] stats, int numGroups) {
            throw new IllegalArgumentException("Cannot use TermEquals in a getGroupStats");
        }

        @Override
        public boolean allow(String term, long[] stats, int group) {
            return term.equals(value.stringTerm);
        }

        @Override
        public boolean allow(long term, long[] stats, int group) {
            return term == value.intTerm;
        }
    }

    class Not implements AggregateFilter {
        private final AggregateFilter f;

        public Not(AggregateFilter f) {
            this.f = f;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return f.requires();
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
            f.register(metricIndexes, groupKeys);
        }

        @Override
        public boolean[] getGroupStats(long[][] stats, int numGroups) {
            final boolean[] inner = f.getGroupStats(stats, numGroups);
            final boolean[] copy = Arrays.copyOf(inner, inner.length);
            for (int i = 0; i < copy.length; i++) {
                copy[i] = !copy[i];
            }
            return copy;
        }

        @Override
        public boolean allow(String term, long[] stats, int group) {
            return !f.allow(term, stats, group);
        }

        @Override
        public boolean allow(long term, long[] stats, int group) {
            return !f.allow(term, stats, group);
        }
    }

    class MetricEquals implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public MetricEquals(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Sets.union(m1.requires(), m2.requires());
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
            m1.register(metricIndexes, groupKeys);
            m2.register(metricIndexes, groupKeys);
        }

        @Override
        public boolean[] getGroupStats(long[][] stats, int numGroups) {
            final double[] l = m1.getGroupStats(stats, numGroups);
            final double[] r = m2.getGroupStats(stats, numGroups);
            final boolean[] result = new boolean[numGroups + 1];
            for (int i = 1; i <= numGroups; i++) {
                result[i] = l[i] == r[i];
            }
            return result;
        }

        @Override
        public boolean allow(String term, long[] stats, int group) {
            return m1.apply(term, stats, group) == m2.apply(term, stats, group);
        }

        @Override
        public boolean allow(long term, long[] stats, int group) {
            return m1.apply(term, stats, group) == m2.apply(term, stats, group);
        }
    }

    class MetricNotEquals implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public MetricNotEquals(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Sets.union(m1.requires(), m2.requires());
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
            m1.register(metricIndexes, groupKeys);
            m2.register(metricIndexes, groupKeys);
        }

        @Override
        public boolean[] getGroupStats(long[][] stats, int numGroups) {
            final double[] l = m1.getGroupStats(stats, numGroups);
            final double[] r = m2.getGroupStats(stats, numGroups);
            final boolean[] result = new boolean[numGroups + 1];
            for (int i = 1; i <= numGroups; i++) {
                result[i] = l[i] != r[i];
            }
            return result;
        }

        @Override
        public boolean allow(String term, long[] stats, int group) {
            return m1.apply(term, stats, group) != m2.apply(term, stats, group);
        }

        @Override
        public boolean allow(long term, long[] stats, int group) {
            return m1.apply(term, stats, group) != m2.apply(term, stats, group);
        }
    }

    class GreaterThan implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public GreaterThan(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Sets.union(m1.requires(), m2.requires());
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
            m1.register(metricIndexes, groupKeys);
            m2.register(metricIndexes, groupKeys);
        }

        @Override
        public boolean[] getGroupStats(long[][] stats, int numGroups) {
            final double[] l = m1.getGroupStats(stats, numGroups);
            final double[] r = m2.getGroupStats(stats, numGroups);
            final boolean[] result = new boolean[numGroups + 1];
            for (int i = 1; i <= numGroups; i++) {
                result[i] = l[i] > r[i];
            }
            return result;
        }

        @Override
        public boolean allow(String term, long[] stats, int group) {
            return m1.apply(term, stats, group) > m2.apply(term, stats, group);
        }

        @Override
        public boolean allow(long term, long[] stats, int group) {
            return m1.apply(term, stats, group) > m2.apply(term, stats, group);
        }
    }

    class GreaterThanOrEquals implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public GreaterThanOrEquals(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Sets.union(m1.requires(), m2.requires());
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
            m1.register(metricIndexes, groupKeys);
            m2.register(metricIndexes, groupKeys);
        }

        @Override
        public boolean[] getGroupStats(long[][] stats, int numGroups) {
            final double[] l = m1.getGroupStats(stats, numGroups);
            final double[] r = m2.getGroupStats(stats, numGroups);
            final boolean[] result = new boolean[numGroups + 1];
            for (int i = 1; i <= numGroups; i++) {
                result[i] = l[i] >= r[i];
            }
            return result;
        }

        @Override
        public boolean allow(String term, long[] stats, int group) {
            return m1.apply(term, stats, group) >= m2.apply(term, stats, group);
        }

        @Override
        public boolean allow(long term, long[] stats, int group) {
            return m1.apply(term, stats, group) >= m2.apply(term, stats, group);
        }
    }

    class LessThan implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public LessThan(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Sets.union(m1.requires(), m2.requires());
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
            m1.register(metricIndexes, groupKeys);
            m2.register(metricIndexes, groupKeys);
        }

        @Override
        public boolean[] getGroupStats(long[][] stats, int numGroups) {
            final double[] l = m1.getGroupStats(stats, numGroups);
            final double[] r = m2.getGroupStats(stats, numGroups);
            final boolean[] result = new boolean[numGroups + 1];
            for (int i = 1; i <= numGroups; i++) {
                result[i] = l[i] < r[i];
            }
            return result;
        }

        @Override
        public boolean allow(String term, long[] stats, int group) {
            return m1.apply(term, stats, group) < m2.apply(term, stats, group);
        }

        @Override
        public boolean allow(long term, long[] stats, int group) {
            return m1.apply(term, stats, group) < m2.apply(term, stats, group);
        }
    }

    class LessThanOrEquals implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public LessThanOrEquals(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Sets.union(m1.requires(), m2.requires());
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
            m1.register(metricIndexes, groupKeys);
            m2.register(metricIndexes, groupKeys);
        }

        @Override
        public boolean[] getGroupStats(long[][] stats, int numGroups) {
            final double[] l = m1.getGroupStats(stats, numGroups);
            final double[] r = m2.getGroupStats(stats, numGroups);
            final boolean[] result = new boolean[numGroups + 1];
            for (int i = 1; i <= numGroups; i++) {
                result[i] = l[i] <= r[i];
            }
            return result;
        }

        @Override
        public boolean allow(String term, long[] stats, int group) {
            return m1.apply(term, stats, group) <= m2.apply(term, stats, group);
        }

        @Override
        public boolean allow(long term, long[] stats, int group) {
            return m1.apply(term, stats, group) <= m2.apply(term, stats, group);
        }
    }

    class And implements AggregateFilter {
        private final AggregateFilter f1;
        private final AggregateFilter f2;

        public And(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Sets.union(f1.requires(), f2.requires());
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
            f1.register(metricIndexes, groupKeys);
            f2.register(metricIndexes, groupKeys);
        }

        @Override
        public boolean[] getGroupStats(long[][] stats, int numGroups) {
            final boolean[] l = f1.getGroupStats(stats, numGroups);
            final boolean[] r = f2.getGroupStats(stats, numGroups);
            final boolean[] result = new boolean[numGroups + 1];
            for (int i = 0; i <= numGroups; i++) {
                result[i] = l[i] && r[i];
            }
            return result;
        }

        @Override
        public boolean allow(String term, long[] stats, int group) {
            return f1.allow(term, stats, group) && f2.allow(term, stats, group);
        }

        @Override
        public boolean allow(long term, long[] stats, int group) {
            return f1.allow(term, stats, group) && f2.allow(term, stats, group);
        }
    }

    class Or implements AggregateFilter {
        private final AggregateFilter f1;
        private final AggregateFilter f2;

        public Or(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Sets.union(f1.requires(), f2.requires());
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
            f1.register(metricIndexes, groupKeys);
            f2.register(metricIndexes, groupKeys);
        }

        @Override
        public boolean[] getGroupStats(long[][] stats, int numGroups) {
            return new boolean[0];
        }

        @Override
        public boolean allow(String term, long[] stats, int group) {
            return f1.allow(term, stats, group) || f2.allow(term, stats, group);
        }

        @Override
        public boolean allow(long term, long[] stats, int group) {
            return f1.allow(term, stats, group) || f2.allow(term, stats, group);
        }
    }

    class RegexFilter implements AggregateFilter {
        private final String field;
        private final String regex;

        private final Pattern pattern;

        public RegexFilter(String field, String regex) {
            this.field = field;
            this.regex = regex;
            this.pattern = Pattern.compile(regex);
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Collections.emptySet();
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
        }

        @Override
        public boolean[] getGroupStats(long[][] stats, int numGroups) {
            throw new UnsupportedOperationException("Cannot use getGroupStats on a RegexFilter");
        }

        @Override
        public boolean allow(String term, long[] stats, int group) {
            return pattern.matcher(term).matches();
        }

        @Override
        public boolean allow(long term, long[] stats, int group) {
            return pattern.matcher(String.valueOf(term)).matches();
        }
    }

    class Constant implements AggregateFilter {
        private final boolean value;

        public Constant(boolean value) {
            this.value = value;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Collections.emptySet();
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
        }

        @Override
        public boolean[] getGroupStats(long[][] stats, int numGroups) {
            final boolean[] result = new boolean[numGroups + 1];
            Arrays.fill(result, value);
            return result;
        }

        @Override
        public boolean allow(String term, long[] stats, int group) {
            return value;
        }

        @Override
        public boolean allow(long term, long[] stats, int group) {
            return value;
        }
    }
}
