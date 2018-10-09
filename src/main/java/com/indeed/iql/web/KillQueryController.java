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

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @author dcahill
 */
@Controller
public class KillQueryController {
    private static final Logger log = Logger.getLogger(KillQueryController.class);
    private final IQLDB iqldb;

    @Autowired
    public KillQueryController(final IQLDB iqldb) {
        this.iqldb = iqldb;
    }

    @RequestMapping("/killquery")
    protected void doGet(@RequestParam("queryid") final long queryId, @RequestParam("username") final String username, final HttpServletResponse resp) throws IOException {
        if (iqldb != null) {
            iqldb.cancelQuery(queryId);
            final PrintWriter output = new PrintWriter(resp.getOutputStream());
            log.info("Query " + queryId + " marked killed by " + username);
            output.flush();
        }
    }
}
