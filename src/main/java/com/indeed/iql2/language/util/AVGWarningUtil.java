package com.indeed.iql2.language.util;

import com.google.common.collect.ImmutableSortedSet;
import com.indeed.iql2.language.AggregateMetric;
import lombok.Data;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class AVGWarningUtil {
    private static final boolean ALLOW_CONSTANT_DIVIDE = true;

    @Data
    private static class State {
        boolean usesAggOnly = false;
        final Set<String> confusingAggOnly = new HashSet<>(); // Aggregate only but people might be confused
        final Set<String> hasDiff = new HashSet<>(); // Can be parsed as both agg metric and doc metric, and the result is different

        State merge(final State other) {
            usesAggOnly |= other.usesAggOnly;
            confusingAggOnly.addAll(other.confusingAggOnly);
            hasDiff.addAll(other.hasDiff);
            return this;
        }

        static State empty() {
            return new State();
        }

        static State hasDiff(final String operator) {
            final State state = empty();
            state.hasDiff.add(operator);
            return state;
        }

        static State confusing(final String operator) {
            final State state = empty();
            state.confusingAggOnly.add(operator);
            return state;
        }

        static State aggOnly() {
            final State state = empty();
            state.usesAggOnly = true;
            return state;
        }
    }

    private static class SuspiciousOperationExtractor implements AggregateMetric.Visitor<State, RuntimeException> {
        private State recurse(final AggregateMetric children) {
            return children.visit(this);
        }

        @Override
        public State visit(final AggregateMetric.Add add) {
            return add.metrics.stream()
                    .map(this::recurse)
                    .reduce(State.empty(), State::merge);
        }

        @Override
        public State visit(final AggregateMetric.Log log) {
            // log(a+b) != log(a)+log(b)
            return State.hasDiff("LOG").merge(recurse(log.m1));
        }

        @Override
        public State visit(final AggregateMetric.Negate negate) {
            return recurse(negate.m1);
        }

        @Override
        public State visit(final AggregateMetric.Abs abs) {
            // abs(a+b) != abs(a) + abs(b)
            return State.hasDiff("ABS").merge(recurse(abs.m1));
        }

        @Override
        public State visit(final AggregateMetric.Floor floor) {
            // This is also aggregate-only function but it's probably not the intention of the user.
            return State.confusing("FLOOR").merge(recurse(floor.m1));
        }

        @Override
        public State visit(final AggregateMetric.Ceil ceil) {
            // This is also aggregate-only function but it's probably not the intention of the user.
            return State.confusing("CEIL").merge(recurse(ceil.m1));
        }

        @Override
        public State visit(final AggregateMetric.Round round) {
            // This is also aggregate-only function but it's probably not the intention of the user.
            return State.confusing("ROUND").merge(recurse(round.m1));
        }

        @Override
        public State visit(final AggregateMetric.Subtract subtract) {
            return recurse(subtract.m1).merge(recurse(subtract.m2));
        }

        @Override
        public State visit(final AggregateMetric.Multiply multiply) {
            // const * (a+b) = (const * a) + (const * b) (unless overflowed)
            if (multiply.m1 instanceof AggregateMetric.Constant) {
                return recurse(multiply.m2);
            }
            if (multiply.m2 instanceof AggregateMetric.Constant) {
                return recurse(multiply.m1);
            }
            return State.hasDiff("multiplication")
                    .merge(recurse(multiply.m1))
                    .merge(recurse(multiply.m2));
        }

        @Override
        public State visit(final AggregateMetric.Divide divide) {
            final State state = recurse(divide.m1);
            if (!ALLOW_CONSTANT_DIVIDE || !(divide.m2 instanceof AggregateMetric.Constant)) {
                state.hasDiff.add("division");
                state.merge(recurse(divide.m2));
            }
            return state;
        }

        @Override
        public State visit(final AggregateMetric.Modulus modulus) {
            return State.hasDiff("modulo")
                    .merge(recurse(modulus.m1))
                    .merge(recurse(modulus.m2));
        }

        @Override
        public State visit(final AggregateMetric.Power power) {
            return State.hasDiff("exponentiation")
                    .merge(recurse(power.m1))
                    .merge(recurse(power.m2));
        }

        @Override
        public State visit(final AggregateMetric.Parent parent) {
            return State.aggOnly().merge(recurse(parent.metric));
        }

        @Override
        public State visit(final AggregateMetric.Lag lag) {
            return State.aggOnly().merge(recurse(lag.metric));
        }

        @Override
        public State visit(final AggregateMetric.IterateLag iterateLag) {
            return State.aggOnly().merge(recurse(iterateLag.metric));
        }

        @Override
        public State visit(final AggregateMetric.Window window) {
            return State.aggOnly().merge(recurse(window.metric));
        }

        @Override
        public State visit(final AggregateMetric.Qualified qualified) {
            return recurse(qualified.metric);
        }

        @Override
        public State visit(final AggregateMetric.DocStatsPushes docStatsPushes) {
            // This should came from extractPrecomputed.
            return State.empty();
        }

        @Override
        public State visit(final AggregateMetric.DocStats docStats) {
            // Can be the result of explicit [] or variance, log loss, COUNT(), etc., or auto promotion from doc metric.
            // We distinguish agg only features by checking the original string
            final String docStatString = docStats.getRawInput();
            final String docMetricString = docStats.docMetric.getRawInput();
            if ((docStatString == null) || !docStatString.equals(docMetricString)) {
                return State.aggOnly();
            }
            return State.empty();
        }

        @Override
        public State visit(final AggregateMetric.Constant constant) {
            // AggregateConstant(a) != [a]
            return State.hasDiff("constant");
        }

        @Override
        public State visit(final AggregateMetric.Percentile percentile) {
            return State.aggOnly();
        }

        @Override
        public State visit(final AggregateMetric.Running running) {
            return State.aggOnly().merge(recurse(running.metric));
        }

        @Override
        public State visit(final AggregateMetric.Distinct distinct) {
            return State.aggOnly();
        }

        @Override
        public State visit(final AggregateMetric.Named named) {
            return named.metric.visit(this);
        }

        @Override
        public State visit(final AggregateMetric.NeedsSubstitution needsSubstitution) {
            // Aliases are aggregate metric and people should aware of that.
            return State.empty();
        }

        @Override
        public State visit(final AggregateMetric.GroupStatsLookup groupStatsLookup) {
            // This should only be generated by extractPrecomputed.
            return State.empty();
        }

        @Override
        public State visit(final AggregateMetric.SumAcross sumAcross) {
            return State.aggOnly().merge(recurse(sumAcross.metric));
        }

        @Override
        public State visit(final AggregateMetric.IfThenElse ifThenElse) {
            // We also have DocMetric.IfThenElse.
            return State.hasDiff("IF-THEN-ELSE")
                    .merge(recurse(ifThenElse.falseCase))
                    .merge(recurse(ifThenElse.falseCase));
        }

        @Override
        public State visit(final AggregateMetric.FieldMin fieldMin) {
            return State.aggOnly()
                    .merge(fieldMin.metric.map(this::recurse).orElse(State.empty()));
        }

        @Override
        public State visit(final AggregateMetric.FieldMax fieldMax) {
            return State.aggOnly()
                    .merge(fieldMax.metric.map(this::recurse).orElse(State.empty()));
        }

        @Override
        public State visit(final AggregateMetric.Min min) {
            // We also have DocMetric.MIN.
            return min.metrics.stream()
                    .map(this::recurse)
                    .reduce(State.hasDiff("MIN"), State::merge);
        }

        @Override
        public State visit(final AggregateMetric.Max max) {
            // We also have DocMetric.Max.
            return max.metrics.stream()
                    .map(this::recurse)
                    .reduce(State.hasDiff("MAX"), State::merge);
        }

        @Override
        public State visit(final AggregateMetric.DivideByCount divideByCount) {
            return State.aggOnly().merge(recurse(divideByCount.metric));
        }
    }

    public static Set<String> extractSuspiciousOperations(final AggregateMetric aggregateMetric) {
        final State state = aggregateMetric.visit(new SuspiciousOperationExtractor());
        if (state.usesAggOnly) {
            // If there is any use of agg-only feature, it's user's intention.
            return Collections.emptySet();
        }
        return ImmutableSortedSet.<String>naturalOrder()
                .addAll(state.confusingAggOnly)
                .addAll(state.hasDiff)
                .build();
    }
}
