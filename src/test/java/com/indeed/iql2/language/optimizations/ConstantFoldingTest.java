package com.indeed.iql2.language.optimizations;

import com.google.common.collect.ImmutableList;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.DocMetric.Abs;
import com.indeed.iql2.language.DocMetric.Add;
import com.indeed.iql2.language.DocMetric.Constant;
import com.indeed.iql2.language.DocMetric.Count;
import com.indeed.iql2.language.DocMetric.Divide;
import com.indeed.iql2.language.DocMetric.Exponentiate;
import com.indeed.iql2.language.DocMetric.Field;
import com.indeed.iql2.language.DocMetric.IfThenElse;
import com.indeed.iql2.language.DocMetric.Log;
import com.indeed.iql2.language.DocMetric.Max;
import com.indeed.iql2.language.DocMetric.MetricEqual;
import com.indeed.iql2.language.DocMetric.MetricGt;
import com.indeed.iql2.language.DocMetric.MetricGte;
import com.indeed.iql2.language.DocMetric.MetricLt;
import com.indeed.iql2.language.DocMetric.MetricLte;
import com.indeed.iql2.language.DocMetric.MetricNotEqual;
import com.indeed.iql2.language.DocMetric.Min;
import com.indeed.iql2.language.DocMetric.Modulus;
import com.indeed.iql2.language.DocMetric.Multiply;
import com.indeed.iql2.language.DocMetric.Negate;
import com.indeed.iql2.language.DocMetric.Signum;
import com.indeed.iql2.language.DocMetric.Subtract;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.util.core.Pair;
import org.junit.Test;

import java.util.List;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;

public class ConstantFoldingTest {
    private static DocMetric constant(final long value) {
        return new Constant(value);
    }

    private static DocMetric field(final String fieldName) {
        return new Field(FieldSet.of("", fieldName, true));
    }

    private static void testFolding(final DocMetric expected, final DocMetric beforeFolding) {
        assertEquals(expected, ConstantFolding.apply(beforeFolding));
    }

    @Test
    public void testRecursiveFolding() {
        testFolding(
                constant(4),
                Add.create(
                        new Subtract(// 2
                                constant(3),
                                constant(1)
                        ),
                        new Multiply( // 2
                                new Divide( // 2
                                        constant(5),
                                        new Subtract( // 2
                                                new Abs(constant(-3)),
                                                new Count()
                                        )
                                ),
                                Max.create( // 1
                                        Min.create( // 1
                                                constant(1),
                                                constant(2)
                                        ),
                                        Min.create(// 0
                                                constant(0),
                                                constant(3)
                                        )
                                )
                        )
                )
        );
        testFolding(
                field("y"),
                Add.create(new Multiply(constant(0), field("x")), new Multiply(constant(1), field("y")))
        );
        testFolding(
                field("x"),
                new Divide(new Subtract(field("x"), constant(0)), constant(1))
        );
    }

    @Test
    public void testAdd() {
        // Note: this folding actually done in DocMetric.Add::create
        testFolding(
                field("y"),
                Add.create(field("y"), Add.create(constant(-1), constant(1)))
        );
    }

    @Test
    public void testMultiply() {
        testFolding(
                field("x"),
                new Multiply(field("x"), constant(1))
        );
        testFolding(
                constant(0),
                new Multiply(field("x"), constant(0))
        );
        testFolding(
                constant(10),
                new Multiply(constant(2), constant(5))
        );
    }

    @Test
    public void testSubtract() {
        testFolding(
                field("x"),
                new Subtract(field("x"), constant(0))
        );
        testFolding(
                constant(-3),
                new Subtract(constant(2), constant(5))
        );
    }

    @Test
    public void testModulus() {
        testFolding(
                constant(2),
                new Modulus(constant(5), constant(3))
        );
    }

    @Test
    public void testNegate() {
        testFolding(
                constant(-3),
                new Negate(constant(3))
        );
    }

    @Test
    public void testSignum() {
        testFolding(
                constant(-1),
                new Signum(constant(-219))
        );
        testFolding(
                constant(1),
                new Signum(constant(219))
        );
    }

    @Test
    public void testAbs() {
        testFolding(
                constant(5),
                new Abs(constant(-5))
        );
        testFolding(
                new Abs(field("x")),
                new Abs(new Abs(new Abs(field("x"))))
        );
    }

    @Test
    public void testCount() {
        testFolding(
                constant(1),
                new Count()
        );
    }

    @Test
    public void testIfThenElse() {
        testFolding(
                field("x"),
                new IfThenElse(
                        new DocFilter.Always(),
                        field("x"),
                        field("y")
                )
        );
        testFolding(
                field("x"),
                new IfThenElse(
                        new DocFilter.MetricEqual(
                                field("a"),
                                field("b")
                        ),
                        field("x"),
                        field("x")
                )
        );
    }

    @Test
    public void testLog() {
        testFolding(
                constant(1),
                new Log(constant(4), 1)
        );
    }

    @Test
    public void testExponentiate() {
        testFolding(
                constant(2),
                new Exponentiate(constant(1), 1)
        );
    }

    @Test
    public void testMinMax() {
        testFolding(
                constant(1),
                Min.create(
                        constant(10),
                        constant(1)
                )
        );
        testFolding(
                constant(10),
                Max.create(
                        constant(10),
                        constant(1)
                )
        );
    }

    @Test
    public void testComparison() {
        final long[] constants = {0, 1, 2};
        final List<Pair<BiFunction<DocMetric, DocMetric, DocMetric>, BiFunction<Long, Long, Boolean>>> metrics =
                ImmutableList.of(
                        Pair.of(MetricEqual::create, Long::equals),
                        Pair.of(MetricNotEqual::create, (x, y) -> !x.equals(y)),
                        Pair.of(MetricGt::new, (x, y) -> x > y),
                        Pair.of(MetricGte::new, (x, y) -> x >= y),
                        Pair.of(MetricLt::new, (x, y) -> x < y),
                        Pair.of(MetricLte::new, (x, y) -> x <= y)
                );

        for (final Pair<BiFunction<DocMetric, DocMetric, DocMetric>, BiFunction<Long, Long, Boolean>> entry : metrics) {
            final BiFunction<Long, Long, Boolean> meaning = entry.getSecond();
            final BiFunction<DocMetric, DocMetric, DocMetric> metric = entry.getFirst();
            for (final long lhs : constants) {
                for (final long rhs : constants) {
                    testFolding(
                            constant(meaning.apply(lhs, rhs) ? 1 : 0),
                            metric.apply(constant(lhs), constant(rhs))
                    );
                }
            }
        }
    }
}