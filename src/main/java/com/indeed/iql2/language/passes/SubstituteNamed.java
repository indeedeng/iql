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

import com.google.common.base.Joiner;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.query.Query;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Stack;
import java.util.function.Function;

public class SubstituteNamed {
    private SubstituteNamed() {
    }

    public static Query substituteNamedMetrics(Query query, Map<String, AggregateMetric> namedMetrics) {
        return query.transform(
                Function.identity(),
                replaceNamed(namedMetrics),
                Function.identity(),
                Function.identity(),
                Function.identity()
        );
    }

    private static Function<AggregateMetric, AggregateMetric> replaceNamed(final Map<String, AggregateMetric> namedMetrics) {
        final Stack<String> substitutionStack = new Stack<>();
        return new Function<AggregateMetric, AggregateMetric>() {
            @Nullable
            @Override
            public AggregateMetric apply(AggregateMetric input) {
                if (input instanceof AggregateMetric.NeedsSubstitution) {
                    final AggregateMetric.NeedsSubstitution substitution = (AggregateMetric.NeedsSubstitution) input;
                    final String field = substitution.substitutionName;
                    if (substitutionStack.contains(field)) {
                        substitutionStack.push(field);
                        throw new IqlKnownException.ParseErrorException("Hit cycle when doing name replacement: [" + Joiner.on(" -> ").join(substitutionStack) + "]");
                    }
                    substitutionStack.push(field);
                    final AggregateMetric result =
                            namedMetrics
                                    .get(field)
                                    .transform(
                                            this,
                                            Function.identity(),
                                            Function.identity(),
                                            Function.identity(),
                                            Function.identity()
                                    );
                    substitutionStack.pop();
                    return result;
                }
                return input;
            }
        };
    }
}
