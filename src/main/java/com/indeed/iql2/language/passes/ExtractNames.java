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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.Query;

import java.util.HashMap;
import java.util.Map;

public class ExtractNames {
    private ExtractNames() {
    }

    public static Map<String, AggregateMetric> extractNames(Query query) {
        final Map<String, AggregateMetric> result = new HashMap<>();
        query.transform(
                Functions.<GroupBy>identity(),
                handleAggregateMetric(result),
                Functions.<DocMetric>identity(),
                Functions.<AggregateFilter>identity(),
                Functions.<DocFilter>identity()
        );
        return result;
    }

    private static Function<AggregateMetric, AggregateMetric> handleAggregateMetric(final Map<String, AggregateMetric> resultAggregator) {
        return new Function<AggregateMetric, AggregateMetric>() {
            public AggregateMetric apply(AggregateMetric input) {
                if (input instanceof AggregateMetric.Named) {
                    final AggregateMetric.Named named = (AggregateMetric.Named) input;
                    if (resultAggregator.containsKey(named.name.unwrap())) {
                        throw new IqlKnownException.ParseErrorException("Trying to name multiple metrics the same name: [" + named.name.unwrap() + "]!");
                    }
                    resultAggregator.put(named.name.unwrap(), named.metric);
                }
                return input;
            }
        };
    }
}
