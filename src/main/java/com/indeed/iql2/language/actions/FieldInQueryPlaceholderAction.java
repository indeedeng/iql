package com.indeed.iql2.language.actions;

import com.google.common.base.Function;
import com.google.common.base.Objects;
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

/**
 * Exists so that we can transform a Query with subqueries into a
 * List&lt;Command&gt; in order to validate it.
 */
public class FieldInQueryPlaceholderAction implements Action {
    private final FieldSet field;
    private final Query query;
    private final boolean isNegated;
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
    public com.indeed.iql2.execution.actions.Action toExecutionAction(final Function<String, PerGroupConstant> namedMetricLookup, final GroupKeySet groupKeySet) {
        throw new UnsupportedOperationException("Must transform the FieldInQueryPlaceholderAction out before doing a .getExecutionActions()");
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FieldInQueryPlaceholderAction that = (FieldInQueryPlaceholderAction) o;
        return isNegated == that.isNegated &&
                target == that.target &&
                positive == that.positive &&
                negative == that.negative &&
                Objects.equal(field, that.field) &&
                Objects.equal(query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(field, query, isNegated, target, positive, negative);
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
