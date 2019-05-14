package com.indeed.iql2.language.util;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import com.indeed.iql2.language.AggregateMetric;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class AVGWarningUtil {
    private static final boolean ALLOW_CONSTANT_DIVIDE = true;
    private static final boolean ALLOW_AGG_ONLY_FEATURE = true;

    private static class SuspiciousOperationExtractor implements AggregateMetric.Visitor<Set<String>, RuntimeException> {
        @Override
        public Set<String> visit(final AggregateMetric.Add add) {
            return add.metrics.stream()
                    .map(operand -> operand.visit(this))
                    .flatMap(Set::stream)
                    .collect(Collectors.toSet());
        }

        @Override
        public Set<String> visit(final AggregateMetric.Log log) {
            // log(a+b) != log(a)+log(b)
            return Collections.singleton("LOG");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Negate negate) {
            return negate.m1.visit(this);
        }

        @Override
        public Set<String> visit(final AggregateMetric.Abs abs) {
            // abs(a+b) != abs(a) + abs(b)
            return Collections.singleton("ABS");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Floor floor) {
            // This is also aggregate-only function but it's probably not the intention of the user.
            return Collections.singleton("FLOOR");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Ceil ceil) {
            // This is also aggregate-only function but it's probably not the intention of the user.
            return Collections.singleton("CEIL");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Round round) {
            // This is also aggregate-only function but it's probably not the intention of the user.
            return Collections.singleton("ROUND");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Subtract subtract) {
            return Sets.union(subtract.m1.visit(this), subtract.m2.visit(this));
        }

        @Override
        public Set<String> visit(final AggregateMetric.Multiply multiply) {
            // const * (a+b) = (const * a) + (const * b) (unless overflowed)
            if (multiply.m1 instanceof AggregateMetric.Constant) {
                return multiply.m2.visit(this);
            }
            if (multiply.m2 instanceof AggregateMetric.Constant) {
                return multiply.m1.visit(this);
            }
            return Collections.singleton("multiplication");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Divide divide) {
            final ImmutableSet.Builder<String> builder = ImmutableSet.<String>builder()
                    .addAll(divide.m1.visit(this));
            if (!ALLOW_CONSTANT_DIVIDE || !(divide.m2 instanceof AggregateMetric.Constant)) {
                builder.add("division");
                builder.addAll(divide.m2.visit(this));
            }
            return builder.build();
        }

        @Override
        public Set<String> visit(final AggregateMetric.Modulus modulus) {
            return ImmutableSet.<String>builder()
                    .addAll(modulus.m1.visit(this))
                    .add("modulo")
                    .addAll(modulus.m2.visit(this))
                    .build();
        }

        @Override
        public Set<String> visit(final AggregateMetric.Power power) {
            return ImmutableSet.<String>builder()
                    .addAll(power.m1.visit(this))
                    .add("exponentiation")
                    .addAll(power.m2.visit(this))
                    .build();
        }

        @Override
        public Set<String> visit(final AggregateMetric.Parent parent) {
            return ALLOW_AGG_ONLY_FEATURE ? Collections.emptySet() : Collections.singleton("PARENT");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Lag lag) {
            return ALLOW_AGG_ONLY_FEATURE ? Collections.emptySet() : Collections.singleton("LAG");
        }

        @Override
        public Set<String> visit(final AggregateMetric.IterateLag iterateLag) {
            return ALLOW_AGG_ONLY_FEATURE ? Collections.emptySet() : Collections.singleton("LAG");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Window window) {
            return ALLOW_AGG_ONLY_FEATURE ? Collections.emptySet() : Collections.singleton("WINDOW_SUM");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Qualified qualified) {
            return qualified.metric.visit(this);
        }

        @Override
        public Set<String> visit(final AggregateMetric.DocStatsPushes docStatsPushes) {
            // This should came from extractPrecomputed.
            return Collections.emptySet();
        }

        @Override
        public Set<String> visit(final AggregateMetric.DocStats docStats) {
            // This should be the result of explicit [] or variance, log loss, COUNT(), etc.
            return Collections.emptySet();
        }

        @Override
        public Set<String> visit(final AggregateMetric.Constant constant) {
            // AggregateConstant(a) != [a]
            return Collections.singleton("constant");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Percentile percentile) {
            return ALLOW_AGG_ONLY_FEATURE ? Collections.emptySet() : Collections.singleton("PERCENTILE");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Running running) {
            return ALLOW_AGG_ONLY_FEATURE ? Collections.emptySet() : Collections.singleton("RUNNING");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Distinct distinct) {
            return ALLOW_AGG_ONLY_FEATURE ? Collections.emptySet() : Collections.singleton("DISTINCT");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Named named) {
            return named.metric.visit(this);
        }

        @Override
        public Set<String> visit(final AggregateMetric.NeedsSubstitution needsSubstitution) {
            // Aliases are aggregate metric and people should aware of that.
            return Collections.emptySet();
        }

        @Override
        public Set<String> visit(final AggregateMetric.GroupStatsLookup groupStatsLookup) {
            // This should only be generated by extractPrecomputed.
            return Collections.emptySet();
        }

        @Override
        public Set<String> visit(final AggregateMetric.SumAcross sumAcross) {
            return ALLOW_AGG_ONLY_FEATURE ? Collections.emptySet() : Collections.singleton("SUM_OVER");
        }

        @Override
        public Set<String> visit(final AggregateMetric.IfThenElse ifThenElse) {
            // We also have DocMetric.IfThenElse.
            return Collections.singleton("IF-THEN-ELSE");
        }

        @Override
        public Set<String> visit(final AggregateMetric.FieldMin fieldMin) {
            return ALLOW_AGG_ONLY_FEATURE ? Collections.emptySet() : Collections.singleton("FIELD_MIN");
        }

        @Override
        public Set<String> visit(final AggregateMetric.FieldMax fieldMax) {
            return ALLOW_AGG_ONLY_FEATURE ? Collections.emptySet() : Collections.singleton("FIELD_MAX");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Min min) {
            // We also have DocMetric.MIN.
            return Collections.singleton("MIN");
        }

        @Override
        public Set<String> visit(final AggregateMetric.Max max) {
            // We also have DocMetric.Max.
            return Collections.singleton("MAX");
        }

        @Override
        public Set<String> visit(final AggregateMetric.DivideByCount divideByCount) {
            return ALLOW_AGG_ONLY_FEATURE ? Collections.emptySet() : Collections.singleton("AVG");
        }

    }

    public static Set<String> extractSuspiciousOperations(final AggregateMetric aggregateMetric) {
        return ImmutableSortedSet.copyOf(aggregateMetric.visit(new SuspiciousOperationExtractor()));
    }
}
