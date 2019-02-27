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

package com.indeed.iql2.language.actions;

import com.indeed.flamdex.query.BooleanOp;
import com.indeed.flamdex.query.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Actions {
    private Actions() {
    }

    private static boolean groupsAndScopeMatch(QueryAction q1, QueryAction q2) {
        return scopeMatches(q1, q2) && targetMatches(q1, q2) && q1.negativeGroup == q2.negativeGroup && q1.positiveGroup == q2.positiveGroup;
    }

    private static boolean targetMatches(QueryAction q1, QueryAction q2) {
        return q1.targetGroup == q2.targetGroup;
    }

    private static boolean scopeMatches(QueryAction q1, QueryAction q2) {
        return q1.scope().equals(q2.scope());
    }

    private static boolean resultGroupsOppose(QueryAction q1, QueryAction q2) {
        return q1.negativeGroup == q2.positiveGroup && q1.positiveGroup == q2.negativeGroup;
    }

    public static List<Action> optimizeConsecutiveQueryActions(List<Action> actions) {
        final List<Action> result = new ArrayList<>();
        QueryAction currentQueryAction = null;
        for (final Action action : actions) {
            if (action instanceof QueryAction) {
                final QueryAction queryAction = (QueryAction) action;
                if (currentQueryAction == null) {
                    currentQueryAction = queryAction;
                } else if (groupsAndScopeMatch(currentQueryAction, queryAction)) {
                    if (queryAction.targetGroup == queryAction.negativeGroup) {
                        final Map<String, Query> perDatasetQuery = new HashMap<>();
                        for (final String dataset : queryAction.scope()) {
                            perDatasetQuery.put(dataset, Query.newBooleanQuery(BooleanOp.OR, Arrays.asList(currentQueryAction.perDatasetQuery.get(dataset), queryAction.perDatasetQuery.get(dataset))));
                        }
                        currentQueryAction = new QueryAction(perDatasetQuery, queryAction.targetGroup, queryAction.positiveGroup, queryAction.negativeGroup);
                    } else if (queryAction.targetGroup == queryAction.positiveGroup) {
                        final Map<String, Query> perDatasetQuery = new HashMap<>();
                        for (final String dataset : queryAction.scope()) {
                            perDatasetQuery.put(dataset, Query.newBooleanQuery(BooleanOp.AND, Arrays.asList(currentQueryAction.perDatasetQuery.get(dataset), queryAction.perDatasetQuery.get(dataset))));
                        }
                        currentQueryAction = new QueryAction(perDatasetQuery, queryAction.targetGroup, queryAction.positiveGroup, queryAction.negativeGroup);
                    } else {
                        result.add(currentQueryAction);
                        currentQueryAction = queryAction;
                    }
//                } else if (scopeMatches(currentQueryAction, queryAction)
//                            && targetMatches(currentQueryAction, queryAction)
//                            && resultGroupsOppose(currentQueryAction, queryAction)) {
                    // TODO: Make this optimization not suck, and re-enable.
//                    final Map<String, Query> perDatasetQuery = new HashMap<>();
//                    for (final String dataset : queryAction.scope) {
//                        perDatasetQuery.put(
//                                dataset,
//                                Query.newBooleanQuery(
//                                    BooleanOp.NOT,
//                                    Collections.singletonList(
//                                        Query.newBooleanQuery(
//                                            BooleanOp.OR,
//                                            Arrays.asList(
//                                                    Query.newBooleanQuery(BooleanOp.NOT, Collections.singletonList(currentQueryAction.perDatasetQuery.get(dataset))),
//                                                    Query.newBooleanQuery(BooleanOp.NOT, Collections.singletonList(queryAction.perDatasetQuery.get(dataset)))
//                                            )
//                                        )
//                                    )
//                                )
//                        );
//                    }
//                    currentQueryAction = new QueryAction(currentQueryAction.scope, perDatasetQuery, currentQueryAction.targetGroup, currentQueryAction.positiveGroup, currentQueryAction.negativeGroup);
                } else {
                    result.add(currentQueryAction);
                    currentQueryAction = queryAction;
                }
            } else {
                if (currentQueryAction != null) {
                    result.add(currentQueryAction);
                    currentQueryAction = null;
                }
                result.add(action);
            }
        }
        if (currentQueryAction != null) {
            result.add(currentQueryAction);
        }
        return result;
    }
}
