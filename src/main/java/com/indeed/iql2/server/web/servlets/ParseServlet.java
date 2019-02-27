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

package com.indeed.iql2.server.web.servlets;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.ServletUtil;
import com.indeed.iql2.IQL2Options;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.shardresolution.NullShardResolver;
import com.indeed.iql2.language.query.shardresolution.ShardResolver;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.logging.TracingTreeTimer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;

@Controller
public class ParseServlet {

    private final ImhotepMetadataCache metadataCache;
    private final IQL2Options defaultIQL2Options;

    @Autowired
    public ParseServlet(
            final ImhotepMetadataCache metadataCacheIQL2,
            final IQL2Options defaultIQL2Options
    ) {
        this.metadataCache = metadataCacheIQL2;
        this.defaultIQL2Options = defaultIQL2Options;
    }

//    @RequestMapping("parse")
    @ResponseBody
    public Object parse(
            final HttpServletRequest request,
            final HttpServletResponse response,
            @RequestParam("q") @Nonnull final String q
    ) {
        final int version = ServletUtil.getIQLVersionBasedOnParam(request);
        try {
            response.setHeader("Content-Type", "application/json");
            final StoppedClock clock = new StoppedClock();
            final TracingTreeTimer timer = new TracingTreeTimer();
            final ShardResolver shardResolver = new NullShardResolver();
            Queries.parseQuery(q, version == 1, metadataCache.get(), defaultIQL2Options.getOptions(), clock, timer, shardResolver);
            return ImmutableMap.of("parsed", true);
        } catch (Exception e) {
            final HashMap<String, Object> errorMap = new HashMap<>();
            errorMap.put("clause", "where");
            errorMap.put("offsetInClause", 5);
            errorMap.put("exceptionType", e.getClass().getSimpleName());
            errorMap.put("message", e.getMessage());
            errorMap.put("stackTrace", Joiner.on('\n').join(e.getStackTrace()));
            return errorMap;
        }
    }
}
