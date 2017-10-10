package com.indeed.squall.iql2.language.passes;

import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.GroupSuppliers;
import com.indeed.squall.iql2.language.actions.Action;
import com.indeed.squall.iql2.language.actions.Actions;
import com.indeed.squall.iql2.language.execution.ExecutionStep;
import com.indeed.squall.iql2.language.optimizations.ConstantFolding;
import com.indeed.squall.iql2.language.query.Query;

import java.util.Collections;
import java.util.List;

public class HandleWhereClause {
    public static Result handleWhereClause(Query query) {
        if (query.filter.isPresent()) {
            final Query newQuery = new Query(query.datasets, Optional.<DocFilter>absent(), query.groupBys, query.selects, query.formatStrings, query.options, query.rowLimit, query.useLegacy);
            final List<Action> naiveActions = ConstantFolding.apply(query.filter.get()).getExecutionActions(query.nameToIndex(), 1, 1, 0, GroupSuppliers.newGroupSupplier(2));
            // TODO: Should the optimization part happen somewhere else?
            final List<Action> optimizedActions = Actions.optimizeConsecutiveQueryActions(naiveActions);
            return new Result(newQuery, Collections.<ExecutionStep>singletonList(new ExecutionStep.FilterActions(optimizedActions)));
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
