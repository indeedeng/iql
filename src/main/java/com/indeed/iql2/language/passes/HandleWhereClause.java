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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.indeed.iql2.language.actions.Action;
import com.indeed.iql2.language.actions.Actions;
import com.indeed.iql2.language.execution.ExecutionStep;
import com.indeed.iql2.language.optimizations.ConstantFolding;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.GroupSuppliers;
import com.indeed.iql2.language.query.Query;

import java.util.Collections;
import java.util.List;

public class HandleWhereClause {
    public static Result handleWhereClause(Query query) {
        if (query.filter.isPresent()) {
            final Query newQuery = new Query(query.datasets, Optional.<DocFilter>absent(), query.groupBys, query.selects, query.formatStrings, query.options, query.rowLimit, query.useLegacy);
            final List<Action> naiveActions = ConstantFolding.apply(query.filter.get()).getExecutionActions(query.nameToIndex(), 1, 1, 0, GroupSuppliers.newGroupSupplier(2));
            // TODO: Should the optimization part happen somewhere else?
            final List<Action> optimizedActions = Actions.optimizeConsecutiveQueryActions(naiveActions);
            return new Result(newQuery, Collections.<ExecutionStep>singletonList(new ExecutionStep.FilterActions(ImmutableList.copyOf(optimizedActions))));
        } else {
            return new Result(query, Collections.<ExecutionStep>emptyList());
        }
    }

    public static class Result {
        public final Query query;
        public final List<ExecutionStep> steps;

        public Result(Query query, List<ExecutionStep> steps) {
            this.query = query;
            this.steps = steps;
        }
    }
}
