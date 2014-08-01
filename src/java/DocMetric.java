import com.fasterxml.jackson.databind.JsonNode;

import java.util.function.Supplier;

/**
 * @author jwolfe
 */
public interface DocMetric {
    public static DocMetric fromJson(JsonNode node) {
        Supplier<DocMetric> m1 = () -> DocMetric.fromJson(node.get("m1"));
        Supplier<DocMetric> m2 = () -> DocMetric.fromJson(node.get("m2"));
        Supplier<DocMetric> value = () -> DocMetric.fromJson(node.get("value"));
        switch (node.get("type").asText()) {
            case "atom":
                return new BaseMetric(node.get("value").asText());
            case "addition":
                return new Add(m1.get(), m2.get());
            case "subtraction":
                return new Subtract(m1.get(), m2.get());
            case "division":
                return new Divide(m1.get(), m2.get());
            case "multiplication":
                return new Multiply(m1.get(), m2.get());
            case "abs":
                return new Abs(value.get());
            case "signum":
                return new Signum(value.get());
            case "constant":
                return new Constant(node.get("value").asLong());
        }
        throw new RuntimeException("Oops: " + node);
    }

    public static class Add implements DocMetric {
        private final DocMetric m1;
        private final DocMetric m2;

        public Add(DocMetric m1, DocMetric m2) {
            super();
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    public static class Subtract implements DocMetric {
        private final DocMetric m1;
        private final DocMetric m2;

        public Subtract(DocMetric m1, DocMetric m2) {
            super();
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    public static class Multiply implements DocMetric {
        private final DocMetric m1;
        private final DocMetric m2;

        public Multiply(DocMetric m1, DocMetric m2) {
            super();
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    public static class Divide implements DocMetric {
        private final DocMetric m1;
        private final DocMetric m2;

        public Divide(DocMetric m1, DocMetric m2) {
            super();
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    public static class Abs implements DocMetric {
        private final DocMetric value;

        public Abs(DocMetric value) {
            this.value = value;
        }
    }

    public static class Signum implements DocMetric {
        private final DocMetric value;

        public Signum(DocMetric value) {
            this.value = value;
        }
    }

    static class Constant implements DocMetric {
        private final long value;

        public Constant(long value) {
            this.value = value;
        }
    }

    static class BaseMetric implements DocMetric {
        private final String push;

        public BaseMetric(String push) {
            this.push = push;
        }
    }
}
