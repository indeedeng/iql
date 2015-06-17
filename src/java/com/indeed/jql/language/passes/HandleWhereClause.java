package com.indeed.jql.language.passes;

import com.google.common.base.Optional;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.GroupSuppliers;
import com.indeed.jql.language.actions.Action;
import com.indeed.jql.language.actions.Actions;
import com.indeed.jql.language.execution.ExecutionStep;
import com.indeed.jql.language.query.Query;

import java.util.Collections;
import java.util.List;

public class HandleWhereClause {
    public static Result handleWhereClause(Query query) {
        if (query.filter.isPresent()) {
            final Query newQuery = new Query(query.datasets, Optional.<DocFilter>absent(), query.groupBys, query.selects);
            final List<Action> naiveActions = query.filter.get().getExecutionActions(query.extractDatasetNames(), 1, 1, 0, GroupSuppliers.newGroupSupplier(2));
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
