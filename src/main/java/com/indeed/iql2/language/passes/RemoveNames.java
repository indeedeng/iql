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

package com.indeed.iql2.language.passes;

import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.query.Query;

import javax.annotation.Nullable;
import java.util.function.Function;

public class RemoveNames {
    private RemoveNames() {
    }

    public static Query removeNames(Query query) {
        return query.transform(
                Function.identity(),
                removeNames(),
                Function.identity(),
                Function.identity(),
                Function.identity()
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
