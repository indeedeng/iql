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

import com.google.common.collect.ImmutableSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.ToString;


@EqualsAndHashCode
@ToString
public class UnconditionalAction implements Action {
    public final ImmutableSet<String> scope;
    public final int targetGroup;
    public final int newGroup;

    public UnconditionalAction(final ImmutableSet<String> scope, final int targetGroup, final int newGroup) {
        this.scope = scope;
        this.targetGroup = targetGroup;
        this.newGroup = newGroup;
    }

    @Override
    public void validate(ValidationHelper validationHelper, ErrorCollector errorCollector) {

    }

    @Override
    public com.indeed.iql2.execution.actions.Action toExecutionAction() {
        return new com.indeed.iql2.execution.actions.UnconditionalAction(
                scope, targetGroup, newGroup
        );
    }
}
