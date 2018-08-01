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

package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.base.Joiner;
import com.indeed.iql.web.ServletUtil;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.squall.iql2.language.query.Queries;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;

@Controller
public class SplitServlet {
//    @RequestMapping("split")
    @ResponseBody
    public Object split(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final @Nonnull @RequestParam("q") String q
    ) {
        final int version = ServletUtil.getIQLVersionBasedOnParam(request);
        response.setHeader("Content-Type", "application/json");
        try {
            return Queries.parseSplitQuery(q, version == 1, new StoppedClock());
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
