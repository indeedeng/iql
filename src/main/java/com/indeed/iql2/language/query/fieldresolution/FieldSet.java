package com.indeed.iql2.language.query.fieldresolution;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.indeed.iql2.language.AbstractPositional;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.util.FieldExtractor;
import org.antlr.v4.runtime.ParserRuleContext;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author jwolfe
 */
public class FieldSet extends AbstractPositional {
    private final Map<String, String> datasetToField;
    private final boolean isIntField;

    // Whether this fieldset was parsed from something that explicitly restricted it further to
    // a (potentially) smaller scope.
    // Only affects wrap() calls. Does not affect equality. Internal state kind of thing.
    private final boolean restricted;

    private FieldSet(
            final Map<String, String> datasetToField,
            final boolean restricted,
            @Nullable final ParserRuleContext ctx,
            final boolean isIntField
    ) {
        this.datasetToField = ImmutableMap.copyOf(datasetToField);
        this.restricted = restricted;
        if (ctx != null) {
            copyPosition(ctx);
        }
        this.isIntField = isIntField;
    }

    public static FieldSet create(
            final Map<String, String> datasetToField,
            final boolean restricted,
            @Nullable final ParserRuleContext ctx,
            final boolean isIntField
    ) {
        if (datasetToField.size() == 1) {
            final String onlyDataset = Iterables.getOnlyElement(datasetToField.keySet());
            return new SingleField(onlyDataset, datasetToField.get(onlyDataset), restricted, ctx, isIntField);
        }
        return new FieldSet(datasetToField, restricted, ctx, isIntField);
    }

    public static FieldSet of(final String dataset, final String field, final boolean isIntField) {
        return create(ImmutableMap.of(dataset, field), false, null, isIntField);
    }

    public static FieldSet of(final String ds1, final String f1, final String ds2, final String f2, final boolean isIntField) {
        return create(
                ImmutableMap.of(ds1, f1, ds2, f2),
                false,
                null,
                isIntField
        );
    }

    public static FieldSet of(final String ds1, final String f1, final String ds2, final String f2, final String ds3, final String f3, final boolean isIntField) {
        return create(
                ImmutableMap.of(ds1, f1, ds2, f2, ds3, f3),
                false,
                null,
                isIntField
        );
    }

    public boolean isIntField() {
        return isIntField;
    }

    public Set<String> datasets() {
        return datasetToField.keySet();
    }

    public String getOnlyDataset() {
        return Iterables.getOnlyElement(datasetToField.keySet());
    }

    public String getOnlyField() {
        return Iterables.getOnlyElement(datasetToField.values());
    }

    public boolean containsDataset(final String dataset) {
        return datasetToField.containsKey(dataset);
    }

    public String datasetFieldName(final String dataset) {
        final String field = datasetToField.get(dataset);
        if (field == null) {
            throw new IllegalArgumentException("Unknown dataset: " + dataset);
        }
        return field;
    }

    public SingleField subset(final String newDataset) {
        Preconditions.checkArgument(datasetToField.keySet().contains(newDataset));
        return new SingleField(newDataset, datasetToField.get(newDataset), true, null, isIntField);
    }

    public Set<FieldExtractor.DatasetField> datasetFields() {
        return datasetToField.entrySet()
                .stream()
                .map(x -> new FieldExtractor.DatasetField(x.getValue(), x.getKey(), false))
                .collect(Collectors.toSet());
    }

    public AggregateMetric wrap(AggregateMetric metric) {
        if (!restricted) {
            return metric;
        } else {
            return new AggregateMetric.Qualified(new ArrayList<>(datasetToField.keySet()), metric);
        }
    }

    public DocFilter wrap(DocFilter docFilter) {
        if (!restricted) {
            return docFilter;
        } else {
            return new DocFilter.Qualified(new ArrayList<>(datasetToField.keySet()), docFilter);
        }
    }

    public DocMetric wrap(DocMetric docMetric) {
        if (!restricted) {
            return docMetric;
        } else if (datasetToField.size() == 1) {
            final String first = Iterables.getFirst(datasetToField.keySet(), null);
            return new DocMetric.Qualified(first, docMetric);
        } else {
            throw new IllegalArgumentException("Too large scope for a DocMetric!: " + datasetToField.keySet());
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FieldSet fieldSet = (FieldSet) o;
        return Objects.equal(datasetToField, fieldSet.datasetToField) && (isIntField == fieldSet.isIntField);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(datasetToField, isIntField);
    }

    @Override
    public String toString() {
        return "FieldSet{" +
                "datasetToField=" + datasetToField +
                ", isIntField=" + isIntField +
                '}';
    }

    // Special case for one dataset, one field name.
    // It's ok to subclass FieldSet since it's immutable.
    public static class SingleField extends FieldSet {
        public SingleField(
                final String dataset,
                final String field,
                final boolean restricted,
                @Nullable final ParserRuleContext ctx,
                final boolean isIntField) {
            super(ImmutableMap.of(dataset, field), restricted, ctx, isIntField);
        }
    }
}
