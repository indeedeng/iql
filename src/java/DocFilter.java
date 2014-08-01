import com.fasterxml.jackson.databind.JsonNode;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.function.Supplier;

/**
 * @author jwolfe
 */
public interface DocFilter {
    public static void main(String[] args) throws ImhotepOutOfMemoryException {
        final ImhotepClient client = new ImhotepClient("/Users/jwolfe/hosts.txt");
        try (final ImhotepSession session = client.sessionBuilder("organic", DateTime.parse("2014-07-01T00:00:00"), DateTime.parse("2014-07-02T00:00:00")).build()) {
            session.pushStat("count()");
            final long[] totalCount = session.getGroupStats(0);
            System.out.println("totalCount = " + Arrays.toString(totalCount));
            if (false) {
                session.pushStat("sjc");
                session.pushStat("sjc");
                session.pushStat("1");
                session.pushStat("+");
                session.pushStat("-");
                session.metricFilter(1, -1, -1, false);
                final long[] count2 = session.getGroupStats(0);
                System.out.println("count2 = " + Arrays.toString(count2));
            } else {
                new LessThan(new DocMetric.BaseMetric("sjc"), new DocMetric.BaseMetric("ojc")).apply(session, 1);
                final long[] lessThan = session.getGroupStats(0);
                System.out.println("lessThan = " + Arrays.toString(lessThan));
                session.resetGroups();
                new MetricEquals(new DocMetric.BaseMetric("sjc"), new DocMetric.BaseMetric("ojc")).apply(session, 1);
                final long[] equal = session.getGroupStats(0);
                System.out.println("equal = " + Arrays.toString(equal));
                session.resetGroups();
                new GreaterThan(new DocMetric.BaseMetric("sjc"), new DocMetric.BaseMetric("ojc")).apply(session, 1);
                final long[] greaterThan = session.getGroupStats(0);
                System.out.println("greaterThan = " + Arrays.toString(greaterThan));
                session.resetGroups();
            }
        }
    }

    public void apply(ImhotepSession session, int numGroups) throws ImhotepOutOfMemoryException;

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

        @Override
        public void apply(ImhotepSession session, int numGroups) throws ImhotepOutOfMemoryException {
            final GroupRemapRule[] rules = new GroupRemapRule[numGroups];
            for (int group = 1; group <= numGroups; group++) {
                rules[group-1] = new GroupRemapRule(group, new RegroupCondition(field, value.isIntTerm, value.intTerm, value.stringTerm, false), 0, group);
            }
            session.regroup(rules);
        }
    }

    public static class Not implements DocFilter {
        private final DocFilter f;

        public Not(DocFilter f) {
            this.f = f;
        }

        @Override
        public void apply(ImhotepSession session, int numGroups) throws ImhotepOutOfMemoryException {
            throw new UnsupportedOperationException();
        }
    }

    public static class MetricEquals implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricEquals(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public void apply(ImhotepSession session, int numGroups) throws ImhotepOutOfMemoryException {
            session.pushStats(m1.pushes());
            session.pushStats(m2.pushes());
            final int index = session.pushStat("-") - 1;
            session.metricFilter(index, 0, 0, false);
            session.popStat();
        }
    }

    public static class GreaterThan implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public GreaterThan(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public void apply(ImhotepSession session, int numGroups) throws ImhotepOutOfMemoryException {
            session.pushStats(m1.pushes());
            session.pushStats(m2.pushes());
            final int index = session.pushStat("-") - 1;
            session.pushStat("1");
            session.pushStat("min()");
            session.metricFilter(index, 1, 1, false);
            session.popStat();
        }
    }

    public static class LessThan implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public LessThan(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public void apply(ImhotepSession session, int numGroups) throws ImhotepOutOfMemoryException {
            session.pushStats(m1.pushes());
            session.pushStats(m2.pushes());
            final int index = session.pushStat("-") - 1;
            session.pushStats(Arrays.asList("0","1","-"));
            session.pushStat("max()");
            session.metricFilter(index, -1, -1, false);
            session.popStat();
        }
    }

    public static class And implements DocFilter {
        private final DocFilter f1;
        private final DocFilter f2;

        public And(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public void apply(ImhotepSession session, int numGroups) throws ImhotepOutOfMemoryException {
            f1.apply(session, numGroups);
            f2.apply(session, numGroups);
        }
    }

    public static class Or implements DocFilter {
        private final DocFilter f1;
        private final DocFilter f2;

        public Or(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public void apply(ImhotepSession session, int numGroups) throws ImhotepOutOfMemoryException {
            throw new UnsupportedOperationException();
        }
    }

    public static class RegexFilter implements DocFilter {
        private final String field;
        private final String regex;

        public RegexFilter(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }

        @Override
        public void apply(ImhotepSession session, int numGroups) throws ImhotepOutOfMemoryException {
            throw new UnsupportedOperationException();
        }
    }
}
