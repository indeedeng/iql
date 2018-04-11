/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
