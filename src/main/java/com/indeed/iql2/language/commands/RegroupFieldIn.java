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

import com.google.common.collect.ImmutableSet;
import com.indeed.iql2.execution.commands.IntRegroupFieldIn;
import com.indeed.iql2.execution.commands.StringRegroupFieldIn;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.Term;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ErrorMessages;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@EqualsAndHashCode
@ToString
public class RegroupFieldIn implements Command {
    private final FieldSet field;
    private final ImmutableSet<Term> terms;
    private final boolean withDefault;

    public RegroupFieldIn(final FieldSet field, final ImmutableSet<Term> terms, final boolean withDefault) {
        this.field = field;
        this.terms = terms;
        this.withDefault = withDefault;
    }

    @Override
    public void validate(final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
        if (field.isIntField()) {
            ValidationUtil.validateIntField(field, validationHelper, errorCollector, this);
            final List<Term> stringTerms = terms.stream().filter(term -> !term.isIntTerm()).collect(Collectors.toList());
            if (!stringTerms.isEmpty()) {
                errorCollector.warn(ErrorMessages.intFieldWithStringTerms(field, stringTerms));
            }
        } else {
            ValidationUtil.validateStringField(field, validationHelper, errorCollector, this);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(
            final Function<String, PerGroupConstant> namedMetricLookup,
            final GroupKeySet groupKeySet,
            final List<String> options) {
        if (field.isIntField()) {
            final LongList intTerms = new LongArrayList(
                    terms.stream().filter(Term::isIntTerm).map(t -> t.intTerm).iterator());
            return new IntRegroupFieldIn(field, intTerms, withDefault);
        } else {
            final List<String> stringTerms = terms.stream().map(Term::asString).collect(Collectors.toList());
            return new StringRegroupFieldIn(field, stringTerms, withDefault);
        }
    }
}
