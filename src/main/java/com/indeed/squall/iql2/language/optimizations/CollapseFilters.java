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

package com.indeed.squall.iql2.language.optimizations;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.Query;

import java.util.ArrayList;
import java.util.List;

public class CollapseFilters {
    public static Query collapseFilters(Query query) {
        return query.transform(
                Functions.<GroupBy>identity(),
                Functions.<AggregateMetric>identity(),
                Functions.<DocMetric>identity(),
                Functions.<AggregateFilter>identity(),
                new Function<DocFilter, DocFilter>() {
                    @Override
                    public DocFilter apply(DocFilter input) {
                        if (isOr(input)) {
                            final List<DocFilter> children = orChildren(input);
                            final List<DocFilter> newChildren = new ArrayList<DocFilter>();
                            for (final DocFilter child : children) {
                                if (isOr(child)) {
                                    newChildren.addAll(orChildren(child));
                                } else {
                                    newChildren.add(child);
                                }
                            }
                            if (children.size() == newChildren.size()) {
                                return input;
                            } else {
                                return apply(new DocFilter.Ors(newChildren));
                            }
                        }
                        return input;
                    }
                }
        );
    }

    private static boolean isOr(DocFilter filter) {
        return filter instanceof DocFilter.Or || filter instanceof DocFilter.Ors;
    }

    private static List<DocFilter> orChildren(DocFilter filter) {
        if (filter instanceof DocFilter.Or) {
            final DocFilter.Or or = (DocFilter.Or) filter;
            return ImmutableList.of(or.f1, or.f2);
        } else if (filter instanceof DocFilter.Ors) {
            return ((DocFilter.Ors) filter).filters;
        }
        throw new IllegalArgumentException("filter argument must satisfy isOr()");
    }
}
