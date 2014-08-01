import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * @author jwolfe
 */
public interface AggregateFilter {
    public Set<List<String>> requires();
    public void register(Map<List<String>, Integer> metricIndexes);
    public boolean allow(String term, long[] stats);
    public boolean allow(long term, long[] stats);

    public static AggregateFilter fromJson(JsonNode node) {
        Supplier<AggregateMetric> m1 = () -> AggregateMetric.fromJson(node.get("arg1"));
        Supplier<AggregateMetric> m2 = () -> AggregateMetric.fromJson(node.get("arg2"));
        Supplier<AggregateFilter> f1 = () -> AggregateFilter.fromJson(node.get("arg1"));
        Supplier<AggregateFilter> f2 = () -> AggregateFilter.fromJson(node.get("arg2"));
        switch (node.get("type").asText()) {
            case "fieldEquals":
                return new FieldEquals(node.get("field").asText(), Term.fromJson(node.get("value")));
            case "not":
                return new Not(AggregateFilter.fromJson(node.get("value")));
            case "regex":
                return new RegexFilter(node.get("field").asText(), node.get("value").asText());
            case "metricEquals":
                return new MetricEquals(m1.get(), m2.get());
            case "greaterThan":
                return new GreaterThan(m1.get(), m2.get());
            case "lessThan":
                return new LessThan(m1.get(), m2.get());
            case "and":
                return new And(f1.get(), f2.get());
            case "or":
                return new Or(f1.get(), f2.get());
        }
        throw new RuntimeException("Oops: " + node);
    }

    public static class FieldEquals implements AggregateFilter {
        private final String field;
        private final Term value;

        public FieldEquals(String field, Term value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public Set<List<String>> requires() {
            return Collections.emptySet();
        }

        @Override
        public void register(Map<List<String>, Integer> metricIndexes) {
        }

        @Override
        public boolean allow(String term, long[] stats) {
            return term.equals(value.stringTerm);
        }

        @Override
        public boolean allow(long term, long[] stats) {
            return Long.compare(term, value.intTerm) == 0;
        }
    }

    public static class Not implements AggregateFilter {
        private final AggregateFilter f;

        public Not(AggregateFilter f) {
            this.f = f;
        }

        @Override
        public Set<List<String>> requires() {
            return f.requires();
        }

        @Override
        public void register(Map<List<String>, Integer> metricIndexes) {
            f.register(metricIndexes);
        }

        @Override
        public boolean allow(String term, long[] stats) {
            return !f.allow(term, stats);
        }

        @Override
        public boolean allow(long term, long[] stats) {
            return !f.allow(term, stats);
        }
    }

    public static class MetricEquals implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public MetricEquals(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public Set<List<String>> requires() {
            return Sets.union(m1.requires(), m2.requires());
        }

        @Override
        public void register(Map<List<String>, Integer> metricIndexes) {
            m1.register(metricIndexes);
            m2.register(metricIndexes);
        }

        @Override
        public boolean allow(String term, long[] stats) {
            return m1.apply(term, stats) == m2.apply(term, stats);
        }

        @Override
        public boolean allow(long term, long[] stats) {
            return m1.apply(term, stats) == m2.apply(term, stats);
        }
    }

    public static class GreaterThan implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public GreaterThan(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public Set<List<String>> requires() {
            return Sets.union(m1.requires(), m2.requires());
        }

        @Override
        public void register(Map<List<String>, Integer> metricIndexes) {
            m1.register(metricIndexes);
            m2.register(metricIndexes);
        }

        @Override
        public boolean allow(String term, long[] stats) {
            return m1.apply(term, stats) > m2.apply(term, stats);
        }

        @Override
        public boolean allow(long term, long[] stats) {
            return m1.apply(term, stats) > m2.apply(term, stats);
        }
    }

    public static class LessThan implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public LessThan(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public Set<List<String>> requires() {
            return Sets.union(m1.requires(), m2.requires());
        }

        @Override
        public void register(Map<List<String>, Integer> metricIndexes) {
            m1.register(metricIndexes);
            m2.register(metricIndexes);
        }

        @Override
        public boolean allow(String term, long[] stats) {
            return m1.apply(term, stats) < m2.apply(term, stats);
        }

        @Override
        public boolean allow(long term, long[] stats) {
            return m1.apply(term, stats) < m2.apply(term, stats);
        }
    }

    public static class And implements AggregateFilter {
        private final AggregateFilter f1;
        private final AggregateFilter f2;

        public And(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public Set<List<String>> requires() {
            return Sets.union(f1.requires(), f2.requires());
        }

        @Override
        public void register(Map<List<String>, Integer> metricIndexes) {
            f1.register(metricIndexes);
            f2.register(metricIndexes);
        }

        @Override
        public boolean allow(String term, long[] stats) {
            return f1.allow(term, stats) && f2.allow(term, stats);
        }

        @Override
        public boolean allow(long term, long[] stats) {
            return f1.allow(term, stats) && f2.allow(term, stats);
        }
    }

    public static class Or implements AggregateFilter {
        private final AggregateFilter f1;
        private final AggregateFilter f2;

        public Or(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public Set<List<String>> requires() {
            return Sets.union(f1.requires(), f2.requires());
        }

        @Override
        public void register(Map<List<String>, Integer> metricIndexes) {
            f1.register(metricIndexes);
            f2.register(metricIndexes);
        }

        @Override
        public boolean allow(String term, long[] stats) {
            return f1.allow(term, stats) || f2.allow(term, stats);
        }

        @Override
        public boolean allow(long term, long[] stats) {
            return f1.allow(term, stats) || f2.allow(term, stats);
        }
    }

    public static class RegexFilter implements AggregateFilter {
        private final String field;
        private final String regex;

        private final Pattern pattern;

        public RegexFilter(String field, String regex) {
            this.field = field;
            this.regex = regex;
            this.pattern = Pattern.compile(regex);
        }

        @Override
        public Set<List<String>> requires() {
            return Collections.emptySet();
        }

        @Override
        public void register(Map<List<String>, Integer> metricIndexes) {
        }

        @Override
        public boolean allow(String term, long[] stats) {
            return pattern.matcher(term).matches();
        }

        @Override
        public boolean allow(long term, long[] stats) {
            return false;
        }
    }
}
