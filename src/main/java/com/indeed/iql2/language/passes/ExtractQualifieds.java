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
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

public class ExtractQualifieds {
    public static Set<String> extractDocMetricDatasets(DocMetric docMetric) {
        final Set<String> aggregator = new HashSet<>();
        docMetric.transform(handleDocMetric(aggregator), handledocFilter(aggregator));
        return aggregator;
    }

    private static Function<DocFilter, DocFilter> handledocFilter(final Set<String> aggregator) {
        return new Function<DocFilter, DocFilter>() {
            @Nullable
            @Override
            public DocFilter apply(@Nullable DocFilter input) {
                if (input instanceof DocFilter.Qualified) {
                    aggregator.addAll(((DocFilter.Qualified) input).scope);
                }
                return null;
            }
        };
    }

    private static Function<DocMetric, DocMetric> handleDocMetric(final Set<String> aggregator) {
        return new Function<DocMetric, DocMetric>() {
            @Nullable
            @Override
            public DocMetric apply(@Nullable DocMetric input) {
                if (input instanceof DocMetric.Qualified) {
                    aggregator.add(((DocMetric.Qualified) input).dataset);
                }
                return input;
            }
        };
    }
}
