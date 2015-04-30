package com.indeed.squall.jql;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * @author jwolfe
 */
public interface DocFilter {
    void apply(String name, Session.ImhotepSessionInfo session, int numGroups) throws ImhotepOutOfMemoryException;

    static DocFilter fromJson(JsonNode node) {
        final Supplier<DocMetric> m1 = () -> DocMetric.fromJson(node.get("arg1"));
        final Supplier<DocMetric> m2 = () -> DocMetric.fromJson(node.get("arg2"));
        final Supplier<DocFilter> f1 = () -> DocFilter.fromJson(node.get("arg1"));
        final Supplier<DocFilter> f2 = () -> DocFilter.fromJson(node.get("arg2"));
        switch (node.get("type").textValue()) {
            case "fieldEquals":
                return new FieldEquals(node.get("field").textValue(), Term.fromJson(node.get("value")));
            case "fieldNotEquals":
                return new FieldNotEquals(node.get("field").textValue(), Term.fromJson(node.get("value")));
            case "not":
                return new Not(DocFilter.fromJson(node.get("value")));
            case "regex":
                return new RegexFilter(node.get("field").textValue(), node.get("value").textValue(), false);
            case "notRegex":
                return new RegexFilter(node.get("field").textValue(), node.get("value").textValue(), true);
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
            case "qualified": {
                final List<String> names = Lists.newArrayList();
                final JsonNode namesArr = node.get("names");
                for (int i = 0; i < namesArr.size(); i++) {
                    names.add(namesArr.get(i).textValue());
                }
                return new QualifiedFilter(names, fromJson(node.get("filter")));
            }
        }
        throw new RuntimeException("Oops: " + node);
    }

    class FieldEquals implements DocFilter {
        private final String field;
        private final Term value;

        public FieldEquals(String field, Term value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public void apply(String name, Session.ImhotepSessionInfo session, int numGroups) throws ImhotepOutOfMemoryException {
            final GroupRemapRule[] rules = new GroupRemapRule[numGroups];
            if ((session.intFields.contains(field) && value.isIntTerm) || (session.stringFields.contains(field) && !value.isIntTerm)) {
                for (int group = 1; group <= numGroups; group++) {
                    rules[group-1] = new GroupRemapRule(group, new RegroupCondition(field, value.isIntTerm, value.intTerm, value.stringTerm, false), 0, group);
                }
            } else if (session.intFields.contains(field)) {
                for (int group = 1; group <= numGroups; group++) {
                    rules[group-1] = new GroupRemapRule(group, new RegroupCondition(field, true, Integer.parseInt(value.stringTerm), null ,false), 0, group);
                }
            } else if (session.stringFields.contains(field)) {
                for (int group = 1; group <= numGroups; group++) {
                    rules[group-1] = new GroupRemapRule(group, new RegroupCondition(field, false, 0, String.valueOf(value.intTerm),false), 0, group);
                }
            } else {
                throw new IllegalArgumentException("Field not present in intFields or stringFields: " + field);
            }
            System.out.println("rules = " + rules);
            session.session.regroup(rules);
        }
    }

    class FieldNotEquals implements DocFilter {
        private final String field;
        private final Term value;

        public FieldNotEquals(String field, Term value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public void apply(String name, Session.ImhotepSessionInfo session, int numGroups) throws ImhotepOutOfMemoryException {
            final GroupRemapRule[] rules = new GroupRemapRule[numGroups];
            // TODO: Code duplication
            if ((session.intFields.contains(field) && value.isIntTerm) || (session.stringFields.contains(field) && !value.isIntTerm)) {
                for (int group = 1; group <= numGroups; group++) {
                    rules[group-1] = new GroupRemapRule(group, new RegroupCondition(field, value.isIntTerm, value.intTerm, value.stringTerm, false), group, 0);
                }
            }
            if (session.intFields.contains(field)) {
                for (int group = 1; group <= numGroups; group++) {
                    rules[group-1] = new GroupRemapRule(group, new RegroupCondition(field, true, Integer.parseInt(value.stringTerm), null ,false), group, 0);
                }
            } else if (session.stringFields.contains(field)) {
                for (int group = 1; group <= numGroups; group++) {
                    rules[group-1] = new GroupRemapRule(group, new RegroupCondition(field, false, 0, String.valueOf(value.intTerm),false), group, 0);
                }
            } else {
                throw new IllegalArgumentException("Field not present in intFields or stringFields: " + field);
            }
            session.session.regroup(rules);
        }
    }

    class Not implements DocFilter {
        private final DocFilter f;

        public Not(DocFilter f) {
            this.f = f;
        }

        @Override
        public void apply(String name, Session.ImhotepSessionInfo session, int numGroups) throws ImhotepOutOfMemoryException {
            throw new UnsupportedOperationException();
        }
    }

    class MetricEquals implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricEquals(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public void apply(String name, Session.ImhotepSessionInfo session, int numGroups) throws ImhotepOutOfMemoryException {
            session.session.pushStats(m1.pushes());
            session.session.pushStats(m2.pushes());
            final int index = session.session.pushStat("-") - 1;
            session.session.metricFilter(index, 0, 0, false);
            session.session.popStat();
        }
    }

    class GreaterThan implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public GreaterThan(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public void apply(String name, Session.ImhotepSessionInfo session, int numGroups) throws ImhotepOutOfMemoryException {
            session.session.pushStats(m1.pushes());
            session.session.pushStats(m2.pushes());
            final int index = session.session.pushStat("-") - 1;
            session.session.pushStat("1");
            session.session.pushStat("min()");
            session.session.metricFilter(index, 1, 1, false);
            session.session.popStat();
        }
    }

    class LessThan implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public LessThan(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public void apply(String name, Session.ImhotepSessionInfo session, int numGroups) throws ImhotepOutOfMemoryException {
            session.session.pushStats(m1.pushes());
            session.session.pushStats(m2.pushes());
            final int index = session.session.pushStat("-") - 1;
            session.session.pushStats(Arrays.asList("0","1","-"));
            session.session.pushStat("max()");
            session.session.metricFilter(index, -1, -1, false);
            session.session.popStat();
        }
    }

    class And implements DocFilter {
        private final DocFilter f1;
        private final DocFilter f2;

        public And(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public void apply(String name, Session.ImhotepSessionInfo session, int numGroups) throws ImhotepOutOfMemoryException {
            f1.apply(name, session, numGroups);
            f2.apply(name, session, numGroups);
        }
    }

    class Or implements DocFilter {
        private final DocFilter f1;
        private final DocFilter f2;

        public Or(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public void apply(String name, Session.ImhotepSessionInfo session, int numGroups) throws ImhotepOutOfMemoryException {
            throw new UnsupportedOperationException();
        }
    }

    class RegexFilter implements DocFilter {
        private final String field;
        private final String regex;
        private final boolean negate;

        public RegexFilter(String field, String regex, boolean negate) {
            this.field = field;
            this.regex = regex;
            this.negate = negate;
        }

        @Override
        public void apply(String name, Session.ImhotepSessionInfo session, int numGroups) throws ImhotepOutOfMemoryException {
            final Pattern pattern = Pattern.compile(regex);
            final FTGSIterator it;
            if (session.intFields.contains(field)) {
                it = session.session.getFTGSIterator(new String[]{field}, new String[]{});
            } else if (session.stringFields.contains(field)) {
                it = session.session.getFTGSIterator(new String[]{}, new String[]{field});
            } else {
                throw new IllegalArgumentException("Unknown field: " + field);
            }
            final DenseInt2ObjectMap<List<String>> stringGroupTerms = new DenseInt2ObjectMap<>();
            final DenseInt2ObjectMap<List<Long>> intGroupTerms = new DenseInt2ObjectMap<>();
            while (it.nextField()) {
                while (it.nextTerm()) {
                    final String term = it.fieldIsIntType() ? String.valueOf(it.termIntVal()) : it.termStringVal();
                    boolean matches = pattern.matcher(term).matches();
                    if (this.negate) {
                        matches = !matches;
                    }
                    if (matches) {
                        while (it.nextGroup()) {
                            final int group = it.group();
                            if (it.fieldIsIntType()) {
                                intGroupTerms.computeIfAbsent(group, ignored -> new LongArrayList()).add(it.termIntVal());
                            } else {
                                stringGroupTerms.computeIfAbsent(group, ignored -> Lists.newArrayList()).add(it.termStringVal());
                            }
                        }
                    }
                }
            }
            final List<GroupMultiRemapRule> rules = Lists.newArrayList();
            for (int group = 1; group <= numGroups; group++) {
                if (stringGroupTerms.containsKey(group)) {
                    final List<String> terms = stringGroupTerms.get(group);
                    final int[] positives = new int[terms.size()];
                    Arrays.fill(positives, group);
                    final RegroupCondition[] conditions = new RegroupCondition[terms.size()];
                    for (int i = 0; i < terms.size(); i++) {
                        final String term = terms.get(i);
                        conditions[i] = new RegroupCondition(field, false, 0, term, false);
                    }
                    rules.add(new GroupMultiRemapRule(group, 0, positives, conditions));
                }
                if (intGroupTerms.containsKey(group)) {
                    final List<Long> terms = intGroupTerms.get(group);
                    final int[] positives = new int[terms.size()];
                    Arrays.fill(positives, group);
                    final RegroupCondition[] conditions = new RegroupCondition[terms.size()];
                    for (int i = 0; i < terms.size(); i++) {
                        final long term = terms.get(i);
                        conditions[i] = new RegroupCondition(field, true, term, null, false);
                    }
                    rules.add(new GroupMultiRemapRule(group, 0, positives, conditions));
                }
            }
            final GroupMultiRemapRule[] rulesArray = rules.toArray(new GroupMultiRemapRule[rules.size()]);
            session.session.regroup(rulesArray);
        }
    }

    class QualifiedFilter implements DocFilter {
        private final Collection<String> name;
        private final DocFilter filter;

        public QualifiedFilter(Collection<String> name, DocFilter filter) {
            this.name = name;
            this.filter = filter;
        }

        @Override
        public void apply(String name, Session.ImhotepSessionInfo session, int numGroups) throws ImhotepOutOfMemoryException {
            if (this.name.contains(name)) {
                filter.apply(name, session, numGroups);
            }
        }
    }
}
