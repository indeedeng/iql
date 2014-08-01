import com.fasterxml.jackson.databind.JsonNode;

import java.util.function.Supplier;

/**
 * @author jwolfe
 */
public interface AggregateFilter {
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
    }

    public static class Not implements AggregateFilter {
        private final AggregateFilter f;

        public Not(AggregateFilter f) {
            this.f = f;
        }
    }

    public static class MetricEquals implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public MetricEquals(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    public static class GreaterThan implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public GreaterThan(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    public static class LessThan implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public LessThan(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    public static class And implements AggregateFilter {
        private final AggregateFilter f1;
        private final AggregateFilter f2;

        public And(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }
    }

    public static class Or implements AggregateFilter {
        private final AggregateFilter f1;
        private final AggregateFilter f2;

        public Or(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

    }

    public static class RegexFilter implements AggregateFilter {
        private final String field;
        private final String regex;

        public RegexFilter(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }
    }
}
