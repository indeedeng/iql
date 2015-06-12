package com.indeed.jql.language;

import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;

import java.util.List;

public class FlamdexQueryTranslator {
    public static DocFilter translate(Query query) {
        switch (query.getQueryType()) {
            case TERM:
                final Term term = query.getStartTerm();
                return new DocFilter.FieldIs(term.getFieldName(), translate(term));
            case BOOLEAN:
                final List<Query> operands = query.getOperands();
                if (operands.isEmpty()) {
                    return new DocFilter.Always();
                }
                DocFilter filter = translate(operands.get(0));
                switch (query.getOperator()) {
                    case AND:
                        for (int i = 1; i < operands.size(); i++) {
                            filter = new DocFilter.And(translate(operands.get(i)), filter);
                        }
                        return filter;
                    case OR:
                        for (int i = 1; i < operands.size(); i++) {
                            filter = new DocFilter.Or(translate(operands.get(i)), filter);
                        }
                        return filter;
                    case NOT:
                        if (operands.size() > 1) {
                            throw new IllegalArgumentException("Found NOT operator with more than one argument?: [" + query + "]");
                        }
                        return new DocFilter.Not(filter);
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
                return new DocFilter.Between(field, start, end);
        }
        throw new UnsupportedOperationException("Unhandled query: [" + query + "]");
    }

    private static com.indeed.jql.language.Term translate(Term term) {
        if (term.isIntField()) {
            return com.indeed.jql.language.Term.term(term.getTermIntVal());
        } else {
            return com.indeed.jql.language.Term.term(term.getTermStringVal());
        }
    }
}
