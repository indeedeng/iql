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
 package com.indeed.iql1.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.indeed.iql.Constants;
import com.indeed.iql.web.ServletUtil;
import com.indeed.iql1.sql.ast2.FromClause;
import com.indeed.iql1.sql.ast2.QueryParts;
import com.indeed.iql1.sql.parser.QuerySplitter;
import com.indeed.iql1.sql.parser.SelectStatementParser;
import com.indeed.iql2.language.query.SplitQuery;
import com.indeed.iql2.server.web.servlets.SplitServlet;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author vladimir
 */
@Controller
public class SplitterServlet {
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(Constants.DEFAULT_IQL_TIME_ZONE);
    private final SplitServlet splitServletV2;

    @Autowired
    public SplitterServlet(SplitServlet splitServletV2) {
        this.splitServletV2 = splitServletV2;
    }

    @RequestMapping("/split")
    @ResponseBody
    public Object doGet(final HttpServletRequest req, final HttpServletResponse resp, @RequestParam("q") String query) {
        if(ServletUtil.getIQLVersionBasedOnParam(req) == 2) {
            return splitServletV2.split(req, resp, query);
        }

        final Object rawSplitResult = splitServletV2.split(req, resp, query);
        if (rawSplitResult instanceof SplitQuery) {
            // Use proper splitter result on success
            return rawSplitResult;
        }

        // Fall back to legacy best guess splitter

        resp.setContentType("application/json");
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode json = mapper.createObjectNode();
        QueryParts parts = null;
        try {
            parts = QuerySplitter.splitQuery(query);
        } catch (Exception e) {
            json.put("error", e.toString());
        }
        if(parts != null) {
            json.put("from", parts.from);
            json.put("where", parts.where);
            json.put("groupBy", parts.groupBy);
            json.put("select", parts.select);
            json.put("limit", parts.limit);

            FromClause fromClause = null;
            try {
                fromClause = SelectStatementParser.parseFromClause(parts.from, DateTime.now(), true);
            } catch (Exception ignored) { }
            json.put("dataset", fromClause != null ? fromClause.getDataset() : "");
            json.put("start", fromClause != null && fromClause.getStart() != null ? fromClause.getStart().toString(dateTimeFormatter) : "");
            json.put("end", fromClause != null && fromClause.getEnd() != null ? fromClause.getEnd().toString(dateTimeFormatter) : "");
            json.put("startRawString", fromClause != null ? fromClause.getStartRawString() : "");
            json.put("endRawString", fromClause != null ? fromClause.getEndRawString() : "");
        }

        return json;
    }
}
