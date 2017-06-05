package com.indeed.squall.iql2.server.web.servlets.query;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.language.DatasetDescriptor;
import com.indeed.squall.iql2.language.FieldDescriptor;
import com.indeed.squall.iql2.language.Positioned;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.commands.Command;
import com.indeed.squall.iql2.language.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.language.dimensions.Dimension;
import com.indeed.squall.iql2.language.query.Dataset;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.iql2.language.util.DatasetsFields;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zheli
 */
public class CommandValidator {
    public static void validate(List<Command> commands, final ImhotepClient imhotepClient, Query query,
                                final Map<String, DatasetDimensions> dimensions, final Map<String, Set<String>> uppercasedDatasetToIntFields,
                                final Set<String> errors, final Set<String> warnings) {
        final Validator validator = new Validator() {
            @Override
            public void error(String error) {
                errors.add(error);
            }

            @Override
            public void warn(String warn) {
                warnings.add(warn);
            }
        };
        final DatasetsFields datasetsFields = buildDatasetsFields(imhotepClient, query.datasets, query.nameToIndex(), dimensions, uppercasedDatasetToIntFields);
        for (final Command command : commands) {
            command.validate(datasetsFields, validator);
        }
    }

    private static DatasetsFields buildDatasetsFields(final ImhotepClient imhotepClient, final List<Dataset> relevantDatasets,
                                                      final Map<String, String> nameToUppercaseDataset,
                                                      final Map<String, DatasetDimensions> uppercasedDimensions,
                                                      final Map<String, Set<String>> uppercasedDatasetToIntFields) {
        final Set<String> relevantUpperCaseDatasets = new HashSet<>();
        for (final Dataset dataset : relevantDatasets) {
            relevantUpperCaseDatasets.add(dataset.dataset.unwrap().toUpperCase());
        }
        final Map<String, String> datasetUpperCaseToActual = new HashMap<>();
        for (final String dataset : Session.getDatasets(imhotepClient)) {
            final String normalized = dataset.toUpperCase();
            if (!relevantUpperCaseDatasets.contains(normalized)) {
                continue;
            }
            if (datasetUpperCaseToActual.containsKey(normalized)) {
                throw new IllegalStateException("Multiple uppercasedDatasets with same uppercase name!");
            }
            datasetUpperCaseToActual.put(normalized, dataset);
        }

        final DatasetsFields.Builder builder = DatasetsFields.builder();
        for (final Map.Entry<String, String> entry : nameToUppercaseDataset.entrySet()) {
            final String dataset = datasetUpperCaseToActual.get(entry.getValue());

            final DatasetInfo datasetInfo = imhotepClient.getDatasetShardInfo(dataset);
            final DatasetDimensions datasetDimension = uppercasedDimensions.get(entry.getValue());
            final Set<String> intFields = uppercasedDatasetToIntFields.get(entry.getValue());

            final DatasetDescriptor datasetDescriptor = DatasetDescriptor.from(datasetInfo,
                    Optional.fromNullable(datasetDimension), intFields);

            final String name = entry.getKey();
            for (final FieldDescriptor fieldDescriptor : datasetDescriptor.getFields()) {
                switch (fieldDescriptor.getType()) {
                    case "Integer":
                        builder.addIntField(name, fieldDescriptor.getName());
                        break;
                    case "String":
                        builder.addStringField(name, fieldDescriptor.getName());
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid FieldDescriptor type: " + fieldDescriptor.getType());
                }
            }

            for (final Dimension dimension : datasetDescriptor.getDimensions()) {
                builder.addMetricField(name, dimension.name, dimension.isAlias);
            }
            builder.addIntField(name, "count()");
        }

        return addDatasetsFieldAliases(builder.build(), relevantDatasets);
    }

    private static DatasetsFields addDatasetsFieldAliases(DatasetsFields datasetsFields, final List<Dataset> relevantDatasets) {
        final Map<String, Dataset> aliasToDataset = Maps.newHashMap();
        for (final Dataset dataset : relevantDatasets) {
            aliasToDataset.put(dataset.alias.or(dataset.dataset).unwrap(), dataset);
        }

        final DatasetsFields.Builder builder = DatasetsFields.builderFrom(datasetsFields);
        for (final String dataset : datasetsFields.uppercasedDatasets()) {
            final ImmutableMap<Positioned<String>, Positioned<String>> aliasToActual = aliasToDataset.get(dataset).fieldAliases;
            for (final Map.Entry<Positioned<String>, Positioned<String>> entry : aliasToActual.entrySet()) {
                final String aliasField = entry.getKey().unwrap();
                final String actualField = entry.getValue().unwrap();
                if (datasetsFields.containsIntField(dataset, actualField) || datasetsFields.containsAliasMetricField(dataset, actualField)) {
                    builder.addIntField(dataset, aliasField);
                } else if (datasetsFields.containsStringField(dataset, actualField)) {
                    builder.addStringField(dataset, aliasField);
                } else {
                    throw new IllegalArgumentException("Alias for non-existent field: " + entry.getValue() + " in dataset " + dataset);
                }
            }
        }
        return builder.build();
    }
}
