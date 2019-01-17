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

package com.indeed.iql2.execution.metrics.aggregate;

import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;

public class Round extends AggregateMetric.FactorUnary {
    public Round(final AggregateMetric m1, final int f1) {
        super(m1, f1);
    }

    @Override
    double eval(final double value, final int digits) {
        final int absDigits = Math.abs(digits);
        final double base = Math.pow(10, absDigits);
        if (digits > 0) {
            return Math.round(value * base) / base;
        }
        else {
            return Math.round(value / base) * base;
        }
    }

    @Override
    AggregateStatTree toImhotep(final AggregateStatTree operand) {
        return operand.round();
    }
}
