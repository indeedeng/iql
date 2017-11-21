package com.indeed.squall.iql2.server.web.servlets.query;

import com.indeed.squall.iql2.language.Positioned;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.commands.Command;
import com.indeed.squall.iql2.language.query.Dataset;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.iql2.language.metadata.DatasetMetadata;
import com.indeed.squall.iql2.language.util.ValidationHelper;
import com.indeed.squall.iql2.language.metadata.DatasetsMetadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zheli
 */
public class CommandValidator {
    public static void validate(final List<Command> commands, final Query query,
                                final DatasetsMetadata datasetsMetadata,
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
        final ValidationHelper validationHelper = buildDatasetsFields(query.datasets, query.nameToIndex(), datasetsMetadata, query.useLegacy);
        for (final Command command : commands) {
            command.validate(validationHelper, validator);
        }
    }

    private static ValidationHelper buildDatasetsFields(final List<Dataset> relevantDatasets, final Map<String, String> nameToActualDataset,
                                                        final DatasetsMetadata datasetsMetadata, final boolean useLegacy) {
        final Map<String, DatasetMetadata> relevantDatasetToMetadata = new HashMap<>();
        final Map<String, Set<String>> relevantDatasetAliasIntFields = new HashMap<>();
        final Map<String, Set<String>> relevantDatasetAliasStringFields = new HashMap<>();
        for (final Dataset relevantDataset : relevantDatasets) {
            final String aliasDataset = relevantDataset.getDisplayName().unwrap();
            final String actualDataset = nameToActualDataset.get(aliasDataset);
            final DatasetMetadata datasetMetada = datasetsMetadata.getMetadata(actualDataset).or(new DatasetMetadata(actualDataset, actualDataset));
            relevantDatasetToMetadata.put(aliasDataset, datasetMetada);

            final Set<String> aliasIntField = new HashSet<>();
            final Set<String> aliasStringField = new HashSet<>();
            for (final Map.Entry<Positioned<String>, Positioned<String>> aliasToFieldEntry : relevantDataset.fieldAliases.entrySet()) {
                final String aliasField = aliasToFieldEntry.getKey().unwrap();
                final String actualField = aliasToFieldEntry.getValue().unwrap();
                if (datasetMetada.fieldToDimension.containsKey(actualField)) {
                    if (!datasetMetada.fieldToDimension.get(actualField).isAlias) {
                        throw new IllegalArgumentException(String.format("Alias for non-alias metric is not supported, metric: %s", actualField));
                    } else {
                        aliasIntField.add(aliasField);
                    }
                } else if (datasetMetada.stringFields.contains(actualField)){
                    aliasStringField.add(aliasField);
                } else if (datasetMetada.intFields.contains(actualField)) {
                    aliasIntField.add(aliasField);
                } else {
                    throw new IllegalArgumentException("Alias for non-existent field: " + actualField + " in dataset " + actualDataset);
                }
            }
            relevantDatasetAliasIntFields.put(aliasDataset, aliasIntField);
            relevantDatasetAliasStringFields.put(aliasDataset, aliasStringField);
        }
        return new ValidationHelper(new DatasetsMetadata(relevantDatasetToMetadata), relevantDatasetAliasIntFields, relevantDatasetAliasStringFields, useLegacy);
    }
}
