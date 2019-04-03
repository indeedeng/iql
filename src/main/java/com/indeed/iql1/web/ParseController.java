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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.indeed.iql.language.IQLStatement;
import com.indeed.iql.language.SelectStatement;
import com.indeed.iql.language.StatementParser;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.QueryServlet;
import com.indeed.iql.web.ServletUtil;
import com.indeed.iql1.sql.parser.SelectStatementParser;
import com.indeed.iql2.server.web.servlets.ParseServlet;
import com.indeed.util.core.time.DefaultWallClock;
import com.indeed.util.core.time.WallClock;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author vladimir
 */

@Controller
public class ParseController {
    private final ImhotepMetadataCache metadata;
    private final ParseServlet parseServletV2;
    private final WallClock wallClock;

    @Autowired
    public ParseController(final ImhotepMetadataCache metadataCacheIQL1, final ParseServlet parseServletV2) {
        this(metadataCacheIQL1, parseServletV2, new DefaultWallClock());
    }

    @VisibleForTesting
    public ParseController(final ImhotepMetadataCache metadataCacheIQL1, final ParseServlet parseServletV2, final WallClock wallClock) {
        this.metadata = metadataCacheIQL1;
        this.parseServletV2 = parseServletV2;
        this.wallClock = wallClock;
    }

    @RequestMapping("/parse")
    @ResponseBody
    public Object handleParse(
            @RequestParam("q") final String query,
            @RequestParam(value = "json", required = false, defaultValue = "") final String json,
            final HttpServletRequest req, final HttpServletResponse resp
    ) throws IOException {

        if (ServletUtil.getIQLVersionBasedOnParam(req) == 2) {
            return parseServletV2.parse(req, resp, query);
        }

        resp.setHeader("Access-Control-Allow-Origin", "*");
        try {
            final IQLStatement parsedQuery = StatementParser.parseIQLToStatement(query);
            if (!(parsedQuery instanceof SelectStatement)) {
                throw new RuntimeException("The query is not recognized as a select statement: " + query);
            }
            return SelectStatementParser.parseSelectStatement(query, new DateTime(wallClock.currentTimeMillis()), metadata);
        } catch (final Throwable e) {
            QueryServlet.handleError(resp, !Strings.isNullOrEmpty(json), e, false, false);
            return null;
        }
    }
}
