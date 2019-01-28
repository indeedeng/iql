package com.indeed.iql2.language.commands;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.cachekeys.CacheKey;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ErrorMessages;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.server.web.servlets.query.CommandValidator;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;

import java.util.List;

/**
 * Exists so that we can transform a Query with subqueries into a
 * List&lt;Command&gt; in order to validate it.
 */
public class GroupByFieldinPlaceholderCommand implements Command {
    private final FieldSet field;
    private final Query query;
    private final DatasetsMetadata datasetsMetadata;

    public GroupByFieldinPlaceholderCommand(final FieldSet field, final Query query, final DatasetsMetadata datasetsMetadata) {
        this.field = field;
        this.query = query;
        this.datasetsMetadata = datasetsMetadata;
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
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(final Function<String, PerGroupConstant> namedMetricLookup, final GroupKeySet groupKeySet, final List<String> options) {
        throw new IllegalStateException("GroupByFieldInQuery must be already transformed into another GroupBy");
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GroupByFieldinPlaceholderCommand that = (GroupByFieldinPlaceholderCommand) o;
        return Objects.equal(field, that.field) &&
                Objects.equal(query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(field, query);
    }

    @Override
    public String toString() {
        return "GroupByFieldinPlaceholderCommand{" +
                "field=" + field +
                ", queryHash=" + CacheKey.computeCacheKey(query).rawHash +
                '}';
    }
}
