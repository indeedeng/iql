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

package com.indeed.iql2.language;

import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;

import java.util.List;
import java.util.stream.Collectors;

public class FlamdexQueryTranslator {
    private FlamdexQueryTranslator() {
    }

    public static DocFilter translate(Query query, final ScopedFieldResolver fieldResolver) {
        switch (query.getQueryType()) {
            case TERM:
                final Term term = query.getStartTerm();
                return DocFilter.FieldIs.create(fieldResolver.resolveContextless(term.getFieldName()), translate(term));
            case BOOLEAN:
                final List<Query> operands = query.getOperands();
                if (operands.isEmpty()) {
                    return new DocFilter.Always();
                }
                final List<DocFilter> translated =
                        operands.stream()
                        .map(operand -> translate(operand, fieldResolver))
                        .collect(Collectors.toList());
                switch (query.getOperator()) {
                    case AND:
                        return DocFilter.And.create(translated);
                    case OR:
                        return DocFilter.Or.create(translated);
                    case NOT:
                        if (operands.size() > 1) {
                            throw new IllegalArgumentException("Found NOT operator with more than one argument?: [" + query + "]");
                        }
                        return new DocFilter.Not(translated.get(0));
                }
            case RANGE:
                final Term startTerm = query.getStartTerm();
                final Term endTerm = query.getEndTerm();
                final String field = startTerm.getFieldName();
                if (!startTerm.isIntField()) {
                    throw new UnsupportedOperationException("Cannot currently translate RANGE lucene queries over Strings");
                }
                final long start = startTerm.getTermIntVal();
                long end = endTerm.getTermIntVal();
                if (query.isMaxInclusive()) {
                    end = end + 1;
                }
                final FieldSet fieldSet = fieldResolver.resolveContextless(field);
                return new DocFilter.Between(new DocMetric.Field(fieldSet), start, end, false);
        }
        throw new UnsupportedOperationException("Unhandled query: [" + query + "]");
    }

    private static com.indeed.iql2.language.Term translate(Term term) {
        if (term.isIntField()) {
            return com.indeed.iql2.language.Term.term(term.getTermIntVal());
        } else {
            return com.indeed.iql2.language.Term.term(term.getTermStringVal());
        }
    }
}
