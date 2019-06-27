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

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.indeed.iql.cache.QueryCache;
import com.indeed.iql.web.QueryServlet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

/**
* @author dwahler
*/
@Controller
public class ResultServlet {

    private final QueryCache queryCache;

    @Autowired
    public ResultServlet(final QueryCache queryCache) {
        this.queryCache = queryCache;
    }

    @RequestMapping("/results/{filename:.+}")
    protected void doGet(final HttpServletResponse resp,
                         @PathVariable("filename") final String filename,
                         @RequestParam(required = false) final String view,
                         final OutputStream outputStream) throws IOException {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        final boolean csv = filename.endsWith(".csv");
        final boolean avoidFileSave = view != null;

        if (!queryCache.isFileCached(filename)) {
            resp.sendError(404);
            return;
        }

        QueryServlet.setContentType(resp, avoidFileSave, csv, false);
        try(final InputStream cacheInputStream = queryCache.getInputStream(filename);
            final PrintWriter printWriter = new PrintWriter(outputStream)) {
            CharStreams.copy(new InputStreamReader(cacheInputStream, Charsets.UTF_8), printWriter);
        }
    }
}
