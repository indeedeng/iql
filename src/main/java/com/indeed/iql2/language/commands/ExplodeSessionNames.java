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

package com.indeed.iql2.language.commands;

import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;

import java.util.List;
import java.util.function.Function;

public class ExplodeSessionNames implements Command {
    @Override
    public void validate(ValidationHelper validationHelper, ErrorCollector errorCollector) {

    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.iql2.execution.commands.ExplodeSessionNames();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ExplodeSessionNames;
    }

    @Override
    public int hashCode() {
        // TODO: ????
        return 3821;
    }

    @Override
    public String toString() {
        return "ExplodeSessionNames{}";
    }
}
