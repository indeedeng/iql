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
 package com.indeed.iql2.language.util;

import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RangeQuery;
import org.apache.lucene.search.TermQuery;

import java.util.Collections;

public final class LuceneQueryFieldRewriter {
    private final String dataset;
    private final ScopedFieldResolver fieldResolver;

    public LuceneQueryFieldRewriter(final String dataset, ScopedFieldResolver fieldResolver) {
        this.dataset = dataset;
        this.fieldResolver = fieldResolver.forScope(Collections.singleton(dataset));
    }

    public Query rewrite(final Query q) {
        if (q instanceof TermQuery) {
            return rewrite((TermQuery)q);
        } else if (q instanceof BooleanQuery) {
            return rewrite((BooleanQuery)q);
        } else if (q instanceof RangeQuery) {
            return rewrite((RangeQuery)q);
        } else if (q instanceof ConstantScoreRangeQuery) {
            return rewrite((ConstantScoreRangeQuery)q);
        } else if (q instanceof PrefixQuery) {
            return rewrite((PrefixQuery)q);
        } else if (q instanceof PhraseQuery) {
            return rewrite((PhraseQuery)q);
        }
        throw new IllegalArgumentException("unsupported lucene query type: " + q.getClass().getSimpleName());
    }

    private PrefixQuery rewrite(final PrefixQuery pq) {
        return new PrefixQuery(rewriteTerm(pq.getPrefix()));
    }

    private BooleanQuery rewrite(final BooleanQuery bq) {
        final BooleanQuery result = new BooleanQuery(bq.isCoordDisabled());
        for (final BooleanClause clause : bq.getClauses()) {
            result.add(rewrite(clause));
        }
        return result;
    }

    private BooleanClause rewrite(final BooleanClause clause) {
        return new BooleanClause(rewrite(clause.getQuery()), clause.getOccur());
    }

    private Query rewrite(final TermQuery tq) {
        return new TermQuery(rewriteTerm(tq.getTerm()));
    }

    private String rewriteField(final String field) {
        return fieldResolver.resolveContextless(field).datasetFieldName(dataset);
    }

    private Term rewriteTerm(final Term lTerm) {
        return new Term(rewriteField(lTerm.field()), lTerm.text());
    }

    private Query rewrite(final RangeQuery rq) {
        return new RangeQuery(rewriteTerm(rq.getLowerTerm()), rewriteTerm(rq.getUpperTerm()), rq.isInclusive(), rq.getCollator());
    }

    private Query rewrite(final ConstantScoreRangeQuery rq) {
        return new ConstantScoreRangeQuery(
                rewriteField(rq.getField()),
                rq.getLowerVal(),
                rq.getUpperVal(),
                rq.includesLower(),
                rq.includesUpper()
        );
    }

    private Query rewrite(final PhraseQuery pq) {
        final PhraseQuery result = new PhraseQuery();
        for (final Term term : pq.getTerms()) {
            result.add(rewriteTerm(term));
        }
        return result;
    }
}
