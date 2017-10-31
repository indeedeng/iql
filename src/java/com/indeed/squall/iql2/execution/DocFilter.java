package com.indeed.squall.iql2.execution;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.squall.iql2.execution.metrics.document.DocMetric;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author jwolfe
 */
public interface DocFilter {
    void apply(String name, Session.ImhotepSessionInfo session, int numGroups) throws ImhotepOutOfMemoryException;

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
                                List<Long> intTerms = intGroupTerms.get(group);
                                if (intTerms == null) {
                                    intTerms = new LongArrayList();
                                    intGroupTerms.put(group, intTerms);
                                }
                                intTerms.add(it.termIntVal());
                            } else {
                                List<String> stringTerms = stringGroupTerms.get(group);
                                if (stringTerms == null) {
                                    stringTerms = Lists.newArrayList();
                                    stringGroupTerms.put(group, stringTerms);
                                }
                                stringTerms.add(it.termStringVal());
                            }
                        }
                    }
                }
            }
            final List<GroupMultiRemapMessage> rules = Lists.newArrayList();
            final RegroupConditionMessage.Builder stringConditionBuilder = RegroupConditionMessage.newBuilder()
                    .setField(field)
                    .setIntType(false)
                    .setIntTerm(0)
                    .setInequality(false);
            final RegroupConditionMessage.Builder intConditionBuilder = RegroupConditionMessage.newBuilder()
                    .setField(field)
                    .setIntType(true)
                    .setInequality(false);
            for (int group = 1; group <= numGroups; group++) {
                if (stringGroupTerms.containsKey(group)) {
                    final List<String> terms = stringGroupTerms.get(group);
                    final int[] positives = new int[terms.size()];
                    Arrays.fill(positives, group);
                    final RegroupConditionMessage[] conditions = new RegroupConditionMessage[terms.size()];
                    for (int i = 0; i < terms.size(); i++) {
                        final String term = terms.get(i);
                        conditions[i] = stringConditionBuilder.setStringTerm(term).build();
                    }
                    rules.add(GroupMultiRemapMessage.newBuilder()
                            .setTargetGroup(group)
                            .setNegativeGroup(0)
                            .addAllPositiveGroup(Ints.asList(positives))
                            .addAllCondition(Arrays.asList(conditions))
                            .build());
                }
                if (intGroupTerms.containsKey(group)) {
                    final List<Long> terms = intGroupTerms.get(group);
                    final int[] positives = new int[terms.size()];
                    Arrays.fill(positives, group);
                    final RegroupConditionMessage[] conditions = new RegroupConditionMessage[terms.size()];
                    for (int i = 0; i < terms.size(); i++) {
                        final long term = terms.get(i);
                        conditions[i] = intConditionBuilder.setIntTerm(term).build();
                    }
                    rules.add(GroupMultiRemapMessage.newBuilder()
                            .setTargetGroup(group)
                            .setNegativeGroup(0)
                            .addAllPositiveGroup(Ints.asList(positives))
                            .addAllCondition(Arrays.asList(conditions))
                            .build());
                }
            }
            final GroupMultiRemapMessage[] rulesArray = rules.toArray(new GroupMultiRemapMessage[rules.size()]);
            session.session.regroupWithProtos(rulesArray, false);
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
