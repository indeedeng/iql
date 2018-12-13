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
import com.indeed.iql2.language.Validator;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import it.unimi.dsi.fastutil.longs.LongList;

import java.util.List;
import java.util.Objects;

public class RegroupFieldIn implements Command {
    private final FieldSet field;
    private final List<String> stringTerms;
    private final LongList intTerms;
    private final boolean isIntField;
    private final boolean withDefault;

    public RegroupFieldIn(FieldSet field, List<String> stringTerms, LongList intTerms, boolean isIntField, boolean withDefault) {
        this.field = field;
        this.stringTerms = stringTerms;
        this.intTerms = intTerms;
        this.isIntField = isIntField;
        this.withDefault = withDefault;
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        if (isIntField) {
            ValidationUtil.validateIntField(field, validationHelper, validator, this);
        } else {
            ValidationUtil.validateStringField(field, validationHelper, validator, this);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegroupFieldIn that = (RegroupFieldIn) o;
        return isIntField == that.isIntField &&
                withDefault == that.withDefault &&
                Objects.equals(field, that.field) &&
                Objects.equals(stringTerms, that.stringTerms) &&
                Objects.equals(intTerms, that.intTerms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, stringTerms, intTerms, isIntField, withDefault);
    }

    @Override
    public String toString() {
        return "RegroupFieldIn{" +
                "field='" + field + '\'' +
                ", stringTerms=" + stringTerms +
                ", intTerms=" + intTerms +
                ", isIntField=" + isIntField +
                ", withDefault=" + withDefault +
                '}';
    }
}
