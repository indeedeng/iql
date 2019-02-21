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

package com.indeed.iql.web;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.print.PrettyPrint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;

@Controller
public class VersionUpgradeServlet {
    private final ImhotepMetadataCache metadataCache;

    @Autowired
    public VersionUpgradeServlet(ImhotepMetadataCache metadataCacheIQL2) {
        this.metadataCache = metadataCacheIQL2;
    }

    @RequestMapping("upgrade")
    @ResponseBody
    public Object upgrade(
            final HttpServletRequest request,
            final HttpServletResponse response,
            @RequestParam("q") @Nonnull final String q
    ) {
        response.setHeader("Content-Type", "application/json");

        try {
            return ImmutableMap.of("upgraded", PrettyPrint.prettyPrint(q, true, metadataCache.get()));
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
