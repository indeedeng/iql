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

package com.indeed.iql1.ez;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql.exceptions.IqlKnownException;

/**
 * @author vladimir
 */

public class ConstantDivideSingleStatReference extends SingleStatReference {
    private final long value;

    public ConstantDivideSingleStatReference(final SingleStatReference stat, final long value) {
        super(stat.depth, stat.toString() + "/" + value);
        if(value == 0) {
            throw new IqlKnownException.ParseErrorException("Can't divide by 0");
        }
        this.value = value;
    }

    @Override
    public double[] getGroupStats(final EZImhotepSession session) throws ImhotepOutOfMemoryException {
        final double[] results = super.getGroupStats(session);
        for(int i = 0; i < results.length; i++) {
            results[i] = results[i] / value;
        }
        return results;
    }

    @Override
    public double getValue(long[] stats) {
        return super.getValue(stats) / value;
    }
}
