import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author jwolfe
 */
public interface DocMetric {
    public List<String> pushes();

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

        @Override
        public List<String> pushes() {
            return binopPushes("+", m1, m2);
        }

    }

    static List<String> binopPushes(String op, DocMetric m1, DocMetric m2) {
        final List<String> result = Lists.newArrayList();
        result.addAll(m1.pushes());
        result.addAll(m2.pushes());
        result.add(op);
        return result;
    }

    public static class Subtract implements DocMetric {
        private final DocMetric m1;
        private final DocMetric m2;

        public Subtract(DocMetric m1, DocMetric m2) {
            super();
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public List<String> pushes() {
            return binopPushes("-", m1, m2);
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

        @Override
        public List<String> pushes() {
            return binopPushes("*", m1, m2);
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

        @Override
        public List<String> pushes() {
            return binopPushes("/", m1, m2);
        }
    }

    public static class Abs implements DocMetric {
        private final DocMetric value;

        public Abs(DocMetric value) {
            this.value = value;
        }

        @Override
        public List<String> pushes() {
            List<String> result = Lists.newArrayList();
            result.addAll(value.pushes());
            result.add("abs()");
            return result;
        }
    }

    public static class Signum implements DocMetric {
        private final DocMetric value;

        public Signum(DocMetric value) {
            this.value = value;
        }

        @Override
        public List<String> pushes() {
            throw new UnsupportedOperationException("unpossible!");
        }
    }

    static class Constant implements DocMetric {
        private final long value;

        public Constant(long value) {
            this.value = value;
        }

        @Override
        public List<String> pushes() {
            return Collections.singletonList(Long.toString(value));
        }
    }

    static class BaseMetric implements DocMetric {
        private final String push;

        public BaseMetric(String push) {
            this.push = push;
        }

        @Override
        public List<String> pushes() {
            return Collections.singletonList(push);
        }
    }
}
