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

package com.indeed.squall.iql2.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.Query;

import javax.annotation.Nullable;

public class RemoveNames {
    public static Query removeNames(Query query) {
        return query.transform(
                Functions.<GroupBy>identity(),
                removeNames(),
                Functions.<DocMetric>identity(),
                Functions.<AggregateFilter>identity(),
                Functions.<DocFilter>identity()
        );
    }

    private static Function<AggregateMetric, AggregateMetric> removeNames() {
        return new Function<AggregateMetric, AggregateMetric>() {
            @Nullable @Override
            public AggregateMetric apply(AggregateMetric input) {
                if (input instanceof AggregateMetric.Named) {
                    return ((AggregateMetric.Named) input).metric;
                } else {
                    return input;
                }
            }
        };
    }
}
