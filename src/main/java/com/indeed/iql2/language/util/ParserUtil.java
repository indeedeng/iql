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

import com.google.common.base.Optional;
import com.indeed.flamdex.lucene.LuceneQueryTranslator;
import com.indeed.flamdex.query.Query;
import com.indeed.iql.metadata.DatasetMetadata;
import com.indeed.iql2.language.metadata.DatasetsMetadata;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;

import java.util.Collections;
import java.util.Set;

/**
 *
 */
public class ParserUtil {

    public static Query getFlamdexQuery(final String query, final String dataset,
                                        final DatasetsMetadata datasetsMeta) {
        final Analyzer analyzer = new KeywordAnalyzer();
        final QueryParser qp = new QueryParser("foo", analyzer);
        qp.setDefaultOperator(QueryParser.Operator.AND);
        final org.apache.lucene.search.Query parsed;
        try {
            parsed = qp.parse(query);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Could not parse lucene term: " + query, e);
        }

        final Optional<DatasetMetadata> metadata = datasetsMeta.getMetadata(dataset);
        if (!metadata.isPresent()) {
            return LuceneQueryTranslator.rewrite(parsed, Collections.<String>emptySet());
        } else {
            final Set<String> intFields = metadata.get().getIntFieldsStringFromMetadata();
            return LuceneQueryTranslator.rewrite(parsed, intFields);
        }
    }

    public static Optional<Interval> getNextNode(ParserRuleContext parserRuleContext) {
        if (parserRuleContext != null && parserRuleContext.getParent() != null) {
            ParserRuleContext cur = parserRuleContext;
            while ((cur.getParent() != null) && (cur.getParent().getChildCount() != 0) &&
                    (cur.getParent().children.indexOf(cur) == (cur.getParent().getChildCount() - 1))) {
                cur = cur.getParent();
            }
            if ((cur.getParent() != null) && (cur.getParent().children.indexOf(cur) != (cur.getParent().getChildCount() - 1))) {
                final ParseTree nextChild = cur.getParent().getChild(cur.getParent().children.indexOf(cur) + 1);
                final int thisStop = parserRuleContext.stop.getStopIndex();
                final int nextStart;
                if (nextChild instanceof ParserRuleContext) {
                    nextStart = ((ParserRuleContext) nextChild).start.getStartIndex();
                } else if (nextChild instanceof TerminalNode) {
                    nextStart = ((TerminalNode) nextChild).getSymbol().getStartIndex();
                } else {
                    nextStart = -1;
                }
                if (nextStart != -1) {
                    if (thisStop + 1 < nextStart - 1) {
                        return Optional.of(Interval.of(thisStop + 1, nextStart - 1));
                    }
                }
            }
        }
            return Optional.absent();
    }

    public static Optional<Interval> getPreviousNode(ParserRuleContext parserRuleContext) {
        if (parserRuleContext != null && parserRuleContext.getParent() != null) {
            ParserRuleContext cur = parserRuleContext;
            while (cur.getParent() != null && cur.getParent().getChildCount() != 0 && cur.getParent().children.indexOf(cur) == 0) {
                cur = cur.getParent();
            }
            if (cur.getParent() != null && cur.getParent().children.indexOf(cur) != 0) {
                final ParseTree prevChild = cur.getParent().getChild(cur.getParent().children.indexOf(cur) - 1);
                final int thisStart = parserRuleContext.start.getStartIndex();
                final int prevStop;
                if (prevChild instanceof ParserRuleContext) {
                    prevStop = ((ParserRuleContext) prevChild).stop.getStopIndex();
                } else if (prevChild instanceof TerminalNode) {
                    prevStop = ((TerminalNode) prevChild).getSymbol().getStopIndex();
                } else {
                    prevStop = -1;
                }

                if (prevStop != -1) {
                    if (prevStop + 1 < thisStart - 1) {
                        return Optional.of(Interval.of(prevStop + 1, thisStart - 1));
                    }
                }
            }
        }
        return Optional.absent();
    }
}
