package com.indeed.squall.iql2.language;

import com.google.common.base.Function;
import junit.framework.Assert;

public class CommonArithmetic {
    /**
     * This mostly exists to ensure we test the same things across aggregate IQLv1, v2, and doc-metrics
     * @param xPlusY result of parsing "X + Y"
     * @param xMinusY result of parsing "X - Y"
     * @param xPlusYMinusZ result of parsing "X + Y - Z"
     * @param xMinusYPlusZ result of parsing "X - Y + Z"
     */
    public static <T> void testAdditivePrecedence(
            Function<String, T> parse,
            T xPlusY,
            T xMinusY,
            T xPlusYMinusZ,
            T xMinusYPlusZ
    ) throws Exception {
        Assert.assertEquals(
                xPlusY,
                parse.apply("X + Y")
        );

        Assert.assertEquals(
                xMinusY,
                parse.apply("X - Y")
        );

        Assert.assertEquals(
                xPlusYMinusZ,
                parse.apply("X + Y - Z")
        );

        Assert.assertEquals(
                xMinusYPlusZ,
                parse.apply("X - Y + Z")
        );
    }

    /**
     * This mostly exists to ensure we test the same things across aggregate IQLv1, v2, and doc-metrics
     *
     * @param aTimesBPlusCTimesD "A * B + C * D"
     * @param lookAtTheJavaDoc "A * B / C * D + (A * B - C * D + E)"
     */
    public static <T> void testLotsOfArithmetic(
            Function<String, T> parse,
            T aTimesBPlusCTimesD,
            T lookAtTheJavaDoc
    ) throws Exception {
        Assert.assertEquals(
                aTimesBPlusCTimesD,
                parse.apply("A * B + C * D")
        );

        Assert.assertEquals(
                lookAtTheJavaDoc,
                parse.apply("A * B / C * D + (A * B - C * D + E)")
        );
    }
}
