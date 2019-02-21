package com.indeed.iql2.language.actions;

import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.execution.ResultFormat;
import com.indeed.iql2.language.cachekeys.CacheKey;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ErrorMessages;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.server.web.servlets.query.CommandValidator;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;

/**
 * Exists so that we can transform a Query with subqueries into a
 * List&lt;Command&gt; in order to validate it.
 */
@EqualsAndHashCode
public class FieldInQueryPlaceholderAction implements Action {
    private final FieldSet field;
    private final Query query;
    private final boolean isNegated;
    @EqualsAndHashCode.Exclude
    private final DatasetsMetadata datasetsMetadata;
    private final int target;
    private final int positive;
    private final int negative;

    public FieldInQueryPlaceholderAction(final FieldSet field, final Query query, final boolean isNegated, final DatasetsMetadata datasetsMetadata, final int target, final int positive, final int negative) {
        this.field = field;
        this.query = query;
        this.isNegated = isNegated;
        this.datasetsMetadata = datasetsMetadata;
        this.target = target;
        this.positive = positive;
        this.negative = negative;
    }

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
    public com.indeed.iql2.execution.actions.Action toExecutionAction() {
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
