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

package com.indeed.iql2.language.actions;

import com.indeed.flamdex.query.Query;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@EqualsAndHashCode
@ToString
public class QueryAction implements Action {
    public final Map<String, Query> perDatasetQuery;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public QueryAction(final Map<String, Query> perDatasetQuery, final int targetGroup, final int positiveGroup, final int negativeGroup) {
        this.perDatasetQuery = perDatasetQuery;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    public Set<String> scope() {
        return Collections.unmodifiableSet(perDatasetQuery.keySet());
    }

    @Override
    public void validate(ValidationHelper validationHelper, ErrorCollector errorCollector) {
        ValidationUtil.validateQuery(validationHelper, perDatasetQuery, errorCollector, this);
    }

    @Override
    public com.indeed.iql2.execution.actions.Action toExecutionAction() {
        return new com.indeed.iql2.execution.actions.QueryAction(
                perDatasetQuery,
                targetGroup,
                positiveGroup,
                negativeGroup
        );
    }
}
