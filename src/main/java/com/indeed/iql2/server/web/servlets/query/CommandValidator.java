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
import com.indeed.iql.web.Limits;
import com.indeed.iql2.language.commands.Command;
import com.indeed.iql2.language.commands.GetGroupStats;
import com.indeed.iql2.language.commands.SimpleIterate;
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.util.core.Pair;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zheli
 */
public class CommandValidator {
    private static final Logger log = Logger.getLogger(CommandValidator.class);

    private CommandValidator() {
    }

    private static class ValidateThrewExceptionException extends RuntimeException {
        public ValidateThrewExceptionException(final Throwable cause) {
            super(cause);
        }
    }

    public static void validate(
            final Query query,
            final Limits limits,
            final DatasetsMetadata datasetsMetadata,
            final ErrorCollector errorCollector
    ) {
        final List<Command> commands = query.commands();
        final ValidationHelper validationHelper = buildValidationHelper(query.datasets, query.nameToIndex(), datasetsMetadata, query.useLegacy, limits);
        for (final Command command : commands) {
            try {
                command.validate(validationHelper, errorCollector);
            } catch (final Exception e) {
                log.error("Validate threw an exception!", new ValidateThrewExceptionException(e));
                errorCollector.error(e.getMessage());
            }
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

    public static ValidationHelper buildValidationHelper(
            final List<Dataset> relevantDatasets,
            final Map<String, String> nameToActualDataset,
            final DatasetsMetadata datasetsMetadata,
            final boolean useLegacy,
            final Limits limits
    ) {
        final Map<String, DatasetMetadata> relevantDatasetToMetadata = new HashMap<>();
        final HashMap<String, Pair<Long,Long>> datasetsTimeRange = new HashMap<>();
        for (final Dataset relevantDataset : relevantDatasets) {
            final String aliasDataset = relevantDataset.getDisplayName();
            final String actualDataset = nameToActualDataset.get(aliasDataset);
            final DatasetMetadata datasetMetadata = datasetsMetadata.getMetadata(actualDataset).orElse(null);
            if (datasetMetadata == null) {
                continue;
            }
            relevantDatasetToMetadata.put(aliasDataset, datasetMetadata);
            datasetsTimeRange.put(aliasDataset,new Pair<>(relevantDataset.startInclusive.unwrap().getMillis(), relevantDataset.endExclusive.unwrap().getMillis()));
        }
        return new ValidationHelper(new DatasetsMetadata(relevantDatasetToMetadata), limits, datasetsTimeRange, useLegacy);
    }
}
