import com.fasterxml.jackson.databind.JsonNode;
import org.apache.log4j.Logger;

import java.util.function.Supplier;

/**
 * @author jwolfe
 */
public interface DocFilter {
    public static DocFilter fromJson(JsonNode node) {
        Supplier<DocMetric> m1 = () -> DocMetric.fromJson(node.get("arg1"));
        Supplier<DocMetric> m2 = () -> DocMetric.fromJson(node.get("arg2"));
        Supplier<DocFilter> f1 = () -> DocFilter.fromJson(node.get("arg1"));
        Supplier<DocFilter> f2 = () -> DocFilter.fromJson(node.get("arg2"));
        switch (node.get("type").asText()) {
            case "fieldEquals":
                return new FieldEquals(node.get("field").asText(), Term.fromJson(node.get("value")));
            case "not":
                return new Not(DocFilter.fromJson(node.get("value")));
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

    public static class FieldEquals implements DocFilter {
        private final String field;
        private final Term value;

        public FieldEquals(String field, Term value) {
            this.field = field;
            this.value = value;
        }
    }

    public static class Not implements DocFilter {
        private final DocFilter f;

        public Not(DocFilter f) {
            this.f = f;
        }
    }

    public static class MetricEquals implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricEquals(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    public static class GreaterThan implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public GreaterThan(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    public static class LessThan implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public LessThan(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    public static class And implements DocFilter {
        private final DocFilter f1;
        private final DocFilter f2;

        public And(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }
    }

    public static class Or implements DocFilter {
        private final DocFilter f1;
        private final DocFilter f2;

        public Or(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

    }

    public static class RegexFilter implements DocFilter {
        private final String field;
        private final String regex;

        public RegexFilter(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }
    }
}
