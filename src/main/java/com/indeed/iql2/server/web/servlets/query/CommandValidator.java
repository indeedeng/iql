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

package com.indeed.iql2.server.web.servlets.query;

import com.indeed.iql.metadata.DatasetMetadata;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.Validator;
import com.indeed.iql2.language.commands.Command;
import com.indeed.iql2.language.commands.GetGroupStats;
import com.indeed.iql2.language.commands.SimpleIterate;
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.util.ValidationHelper;

import java.util.Collections;
import java.util.HashMap;
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

        if (!commands.isEmpty()) {
            final Command lastCommand = commands.get(commands.size() - 1);
            final boolean isExpected = (lastCommand instanceof SimpleIterate) || (lastCommand instanceof GetGroupStats);
            if (!isExpected) {
                throw new IllegalStateException("Last command expected to be SimpleIterate or GetGroupStats. "
                        + "Actual last command: " + lastCommand );
            }
        }
    }

    private static ValidationHelper buildDatasetsFields(final List<Dataset> relevantDatasets, final Map<String, String> nameToActualDataset,
                                                        final DatasetsMetadata datasetsMetadata, final boolean useLegacy) {
        final Map<String, DatasetMetadata> relevantDatasetToMetadata = new HashMap<>();
        for (final Dataset relevantDataset : relevantDatasets) {
            final String aliasDataset = relevantDataset.getDisplayName().unwrap();
            final String actualDataset = nameToActualDataset.get(aliasDataset);
            final DatasetMetadata datasetMetada = datasetsMetadata.getMetadata(actualDataset).orNull();
            if (datasetMetada == null) {
                continue;
            }
            relevantDatasetToMetadata.put(aliasDataset, datasetMetada);
        }
        return new ValidationHelper(new DatasetsMetadata(relevantDatasetToMetadata), Collections.emptyMap(), Collections.emptyMap(), useLegacy);
    }
}
