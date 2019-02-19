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

package com.indeed.iql2.execution.commands;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.TermSelects;
import com.indeed.iql2.execution.AggregateFilter;
import com.indeed.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.iql2.execution.commands.misc.TopK;
import com.indeed.iql2.execution.commands.misc.FieldIterateOpts;
import com.indeed.iql2.execution.commands.SimpleIterate.ResultCollector;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class ComputeFieldExtremeValue implements Command {
    public final FieldSet field;
    public final AggregateMetric metric;
    public final Optional<AggregateFilter> filter;
    public final boolean isFieldMax;

    public ComputeFieldExtremeValue(
        final FieldSet field,
        final AggregateMetric metric,
        final Optional<AggregateFilter> filter,
        final boolean isFieldMax
    ) {
        this.field = field;
        this.metric = metric;
        this.filter = filter;
        this.isFieldMax = isFieldMax;
    }

    @Override
    public void execute(final Session session) {
        // this Command needs special processing since it returns some data.
        throw new IllegalStateException("Call evaluate() method instead");
    }

    public double[] evaluate(final Session session) throws ImhotepOutOfMemoryException, IOException {
        final double[] result = new double[session.numGroups + 1];
        Arrays.fill(result, Double.NaN);
        final ResultCollector out = new ResultCollector() {
            @Override
            public boolean needSortedByGroup() {
                return false;
            }

            @Override
            public boolean offer(final int group, final TermSelects termSelects) {
                if (termSelects.stringTerm != null && !termSelects.stringTerm.isEmpty()) {
                    // TODO: Support string term on FIELD_MAX/FIELD_MIN
                    //
                    try {
                        result[group] = Long.parseLong(termSelects.stringTerm);
                    } catch(NumberFormatException e) {
                        result[group] = Double.NaN;
                    }
                } else {
                    result[group] = termSelects.intTerm;
                }
                return true;
            }

            @Override
            public void finish() {}
        };
        final FieldIterateOpts opts = new FieldIterateOpts();
        opts.topK = Optional.of(new TopK(Optional.of(1), metric, isFieldMax));
        opts.filter = filter;
        new SimpleIterate(field, opts, Collections.emptyList(), Collections.emptyList()).evaluate(session, out);
        return result;
    }
}
