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
import com.indeed.iql2.execution.commands.IntRegroupFieldIn;
import com.indeed.iql2.execution.commands.StringRegroupFieldIn;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@EqualsAndHashCode
@ToString
public class RegroupFieldIn implements Command {
    private final FieldSet field;
    private final List<String> stringTerms;
    private final LongList intTerms;
    private final boolean isIntField;
    private final boolean withDefault;

    public RegroupFieldIn(final FieldSet field, final List<String> stringTerms, final LongList intTerms, final boolean isIntField, final boolean withDefault) {
        this.field = field;
        this.stringTerms = stringTerms;
        this.intTerms = intTerms;
        this.isIntField = isIntField;
        this.withDefault = withDefault;
    }

    @Override
    public void validate(ValidationHelper validationHelper, ErrorCollector errorCollector) {
        if (isIntField) {
            ValidationUtil.validateIntField(field, validationHelper, errorCollector, this);
        } else {
            ValidationUtil.validateStringField(field, validationHelper, errorCollector, this);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        if (isIntField) {
            return new IntRegroupFieldIn(field, intTerms, withDefault);
        } else {
            return new StringRegroupFieldIn(field, stringTerms, withDefault);
        }
    }
}
