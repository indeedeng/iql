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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class IterateAndExplode implements Command {
    public final FieldSet field;
    public final FieldIterateOpts fieldOpts;
    public final Optional<String> explodeDefaultName;

    public IterateAndExplode(FieldSet field, FieldIterateOpts fieldOpts, Optional<String> explodeDefaultName) {
        this.field = field;
        this.fieldOpts = fieldOpts;
        this.explodeDefaultName = explodeDefaultName;
    }

    @Override
    public void validate(ValidationHelper validationHelper, ErrorCollector errorCollector) {
        Preconditions.checkState(validationHelper.datasets().equals(field.datasets()));
        ValidationUtil.validateField(field, validationHelper, errorCollector, this);

        if (fieldOpts.topK.isPresent()) {
            final TopK topK = fieldOpts.topK.get();
            if (topK.metric.isPresent()) {
                topK.metric.get().validate(validationHelper.datasets(), validationHelper, errorCollector);
            }
        }

        if (fieldOpts.filter.isPresent()) {
            fieldOpts.filter.get().validate(validationHelper.datasets(), validationHelper, errorCollector);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.iql2.execution.commands.IterateAndExplode(
                field,
                Collections.emptyList(),
                fieldOpts.toExecution(namedMetricLookup, groupKeySet),
                explodeDefaultName
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IterateAndExplode that = (IterateAndExplode) o;
        return Objects.equals(field, that.field) &&
                Objects.equals(fieldOpts, that.fieldOpts) &&
                Objects.equals(explodeDefaultName, that.explodeDefaultName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, fieldOpts, explodeDefaultName);
    }

    @Override
    public String toString() {
        return "IterateAndExplode{" +
                "field='" + field + '\'' +
                ", fieldOpts=" + fieldOpts +
                ", explodeDefaultName=" + explodeDefaultName +
                '}';
    }
}
