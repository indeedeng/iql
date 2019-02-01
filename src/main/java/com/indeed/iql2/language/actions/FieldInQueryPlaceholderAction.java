package com.indeed.iql2.language.actions;

import com.google.common.base.Function;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.execution.ResultFormat;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.cachekeys.CacheKey;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ErrorMessages;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.server.web.servlets.query.CommandValidator;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

/**
 * Exists so that we can transform a Query with subqueries into a
 * List&lt;Command&gt; in order to validate it.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class FieldInQueryPlaceholderAction implements Action {
    private final FieldSet field;
    private final Query query;
    private final boolean isNegated;
    @EqualsAndHashCode.Exclude
    private final DatasetsMetadata datasetsMetadata;
    private final int target;
    private final int positive;
    private final int negative;

    @Override
    public void validate(final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
        for (final String dataset : field.datasets()) {
            final String fieldName = field.datasetFieldName(dataset);
            if (!validationHelper.containsField(dataset, fieldName)) {
                errorCollector.error(ErrorMessages.missingField(dataset, fieldName, this));
            }
        }
        CommandValidator.validate(query, datasetsMetadata, errorCollector);
    }

    @Override
    public com.indeed.iql2.execution.actions.Action toExecutionAction(final Function<String, PerGroupConstant> namedMetricLookup, final GroupKeySet groupKeySet) {
        throw new UnsupportedOperationException("Must transform the FieldInQueryPlaceholderAction out before doing a .getExecutionActions()");
    }

    @Override
    public String toString() {
        return "FieldInQueryPlaceholderAction{" +
                "field=" + field +
                ", queryHash=" + CacheKey.computeCacheKey(query, ResultFormat.CSV).rawHash +
                ", isNegated=" + isNegated +
                ", target=" + target +
                ", positive=" + positive +
                ", negative=" + negative +
                '}';
    }
}
