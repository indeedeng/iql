package com.indeed.iql2.language.passes;

import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.protobuf.Operator;
import com.indeed.iql2.language.GroupNameSupplier;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import lombok.Data;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface BooleanFilterTree {
    /**
     * Return an equivalent {@link BooleanFilterTree} with all Qualifieds either removed or replaced
     * with their child, as appropriate.
     */
    BooleanFilterTree applyQualifieds(final String dataset);

    String apply(final String dataset, final ImhotepSession session, final GroupNameSupplier groupNameSupplier) throws ImhotepOutOfMemoryException;

    static String applyConsolidation(final List<BooleanFilterTree> children, final String dataset, final ImhotepSession session, final GroupNameSupplier groupNameSupplier, Operator operator) throws ImhotepOutOfMemoryException {
        final List<String> childGroups = new ArrayList<>();
        for (final BooleanFilterTree x : children) {
            childGroups.add(x.apply(dataset, session, groupNameSupplier));
        }
        final String outputGroups = childGroups.get(0);
        session.consolidateGroups(childGroups, operator, outputGroups);
        return outputGroups;
    }

    @Data
    class And implements BooleanFilterTree {
        private final List<BooleanFilterTree> children;

        private And(final List<BooleanFilterTree> children) {
            this.children = children;
        }

        private static BooleanFilterTree of(List<BooleanFilterTree> children) {
            children = children.stream()
                    // inline any direct child ANDs
                    .flatMap(x -> {
                        if (x instanceof And) {
                            return ((And) x).children.stream();
                        } else {
                            return Stream.of(x);
                        }
                    })
                    .collect(Collectors.toList());
            final boolean anyConstantFalse = children.stream().anyMatch(x -> (x instanceof Constant) && !((Constant) x).value);
            if (anyConstantFalse) {
                return new Constant(false);
            }
            final List<BooleanFilterTree> nonConstants = children.stream()
                    .filter(x -> !(x instanceof Constant))
                    .collect(Collectors.toList());
            if (nonConstants.isEmpty()) {
                return new Constant(true);
            }
            return new And(nonConstants);
        }

        @Override
        public BooleanFilterTree applyQualifieds(final String dataset) {
            final List<BooleanFilterTree> newChildren = children.stream()
                    .map(x -> x.applyQualifieds(dataset))
                    .collect(Collectors.toList());
            return And.of(newChildren);
        }

        @Override
        public String apply(final String dataset, final ImhotepSession session, final GroupNameSupplier groupNameSupplier) throws ImhotepOutOfMemoryException {
            return applyConsolidation(children, dataset, session, groupNameSupplier, Operator.AND);
        }
    }

    @Data
    class Or implements BooleanFilterTree {
        private final List<BooleanFilterTree> children;

        private Or(final List<BooleanFilterTree> children) {
            this.children = children;
        }

        private static BooleanFilterTree of(List<BooleanFilterTree> children) {
            children = children.stream()
                    // inline any direct child ORs
                    .flatMap(x -> {
                        if (x instanceof Or) {
                            return ((Or) x).children.stream();
                        } else {
                            return Stream.of(x);
                        }
                    })
                    .collect(Collectors.toList());
            final boolean anyConstantTrue = children.stream().anyMatch(x -> (x instanceof Constant) && ((Constant) x).value);
            if (anyConstantTrue) {
                return new Constant(true);
            }
            final List<BooleanFilterTree> nonConstants = children.stream()
                    .filter(x -> !(x instanceof Constant))
                    .collect(Collectors.toList());
            if (nonConstants.isEmpty()) {
                return new Constant(false);
            }
            return new Or(nonConstants);
        }

        @Override
        public BooleanFilterTree applyQualifieds(final String dataset) {
            final List<BooleanFilterTree> newChildren = children.stream()
                    .map(x -> x.applyQualifieds(dataset))
                    .collect(Collectors.toList());
            return Or.of(newChildren);
        }

        @Override
        public String apply(final String dataset, final ImhotepSession session, final GroupNameSupplier groupNameSupplier) throws ImhotepOutOfMemoryException {
            return applyConsolidation(children, dataset, session, groupNameSupplier, Operator.OR);
        }
    }

    @Data
    class Not implements BooleanFilterTree {
        private final BooleanFilterTree child;

        private Not(final BooleanFilterTree child) {
            this.child = child;
        }

        private static BooleanFilterTree of(final BooleanFilterTree child) {
            if (child instanceof Constant) {
                return new Constant(!((Constant) child).value);
            }
            return new Not(child);
        }

        @Override
        public BooleanFilterTree applyQualifieds(final String dataset) {
            return Not.of(child.applyQualifieds(dataset));
        }

        @Override
        public String apply(final String dataset, final ImhotepSession session, final GroupNameSupplier groupNameSupplier) throws ImhotepOutOfMemoryException {
            return applyConsolidation(Collections.singletonList(child), dataset, session, groupNameSupplier, Operator.NOT);
        }
    }

    /**
     * Anything not in filteredScope is treated as all documents having a TRUE value.
     */
    @Data
    class Qualified implements BooleanFilterTree {
        private final Set<String> filterScope;
        private final BooleanFilterTree child;

        private Qualified(final Set<String> filterScope, final BooleanFilterTree child) {
            this.filterScope = filterScope;
            this.child = child;
        }

        private static BooleanFilterTree of(final Set<String> filterScope, final BooleanFilterTree child) {
            if ((child instanceof Constant) && ((Constant) child).value) {
                return new Constant(true);
            }
            return new Qualified(filterScope, child);
        }

        @Override
        public BooleanFilterTree applyQualifieds(final String dataset) {
            if (filterScope.contains(dataset)) {
                return child;
            } else {
                return new Constant(true);
            }
        }

        @Override
        public String apply(final String dataset, final ImhotepSession session, final GroupNameSupplier groupNameSupplier) {
            throw new IllegalStateException("Cannot apply a Qualified BooleanFilterTree");
        }
    }

    @Data
    class ComputeGroups implements BooleanFilterTree {
        private final String groupsName;
        private final Function<String, ImhotepCommand> command;

        private static ComputeGroups of(final RegroupParams regroupParams, final Function<String, ImhotepCommand> command) {
            return new ComputeGroups(regroupParams.getOutputGroups(), command);
        }

        @Override
        public BooleanFilterTree applyQualifieds(final String dataset) {
            return this;
        }

        @Override
        public String apply(final String dataset, final ImhotepSession session, final GroupNameSupplier groupNameSupplier) throws ImhotepOutOfMemoryException {
            command.apply(dataset).apply(session);
            return groupsName;
        }
    }

    @Data
    class Constant implements BooleanFilterTree {
        private final boolean value;

        @Override
        public BooleanFilterTree applyQualifieds(final String dataset) {
            return this;
        }

        @Override
        public String apply(final String dataset, final ImhotepSession session, final GroupNameSupplier groupNameSupplier) throws ImhotepOutOfMemoryException {
            // TODO: create imhotep operation
            throw new UnsupportedOperationException("You need to implement this");
        }
    }

    @Data
    class FieldInQueryPlaceholder implements BooleanFilterTree {
        private final FieldSet field;
        private final Query query;
        private final boolean isNegated;

        @Override
        public BooleanFilterTree applyQualifieds(final String dataset) {
            throw new UnsupportedOperationException("Must transform the FieldInQueryPlaceholder out before doing a .applyQualifieds()");
        }

        @Override
        public String apply(final String dataset, final ImhotepSession session, final GroupNameSupplier groupNameSupplier) {
            throw new UnsupportedOperationException("Must transform the FieldInQueryPlaceholder out before doing an .apply()");
        }
    }

    static BooleanFilterTree of(final RegroupParams regroupParams, final Function<String, ImhotepCommand> commandFunction) {
        return new ComputeGroups(regroupParams.getOutputGroups(), commandFunction);
    }

    static BooleanFilterTree of(final boolean constant) {
        return new Constant(constant);
    }

    static BooleanFilterTree and(final List<BooleanFilterTree> children) {
        return And.of(children);
    }

    static BooleanFilterTree or(final List<BooleanFilterTree> children) {
        return Or.of(children);
    }

    static BooleanFilterTree not(final BooleanFilterTree child) {
        return Not.of(child);
    }

    static BooleanFilterTree qualified(final Set<String> filterScope, final BooleanFilterTree child) {
        return Qualified.of(filterScope, child);
    }

    static BooleanFilterTree fieldInQueryPlaceholder(final FieldSet field, final Query query, final boolean isNegated) {
        return new FieldInQueryPlaceholder(field, query, isNegated);
    }
}
