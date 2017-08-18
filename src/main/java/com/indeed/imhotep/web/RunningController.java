/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.web;

import com.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;


/**
 * @author vladimir
 */
@Controller
public class RunningController {
    private final RunningQueriesManager runningQueriesManager;
    private final IQLDB iqldb;

    @Autowired
    public RunningController(RunningQueriesManager runningQueriesManager, IQLDB iqldb) {
        this.runningQueriesManager = runningQueriesManager;
        this.iqldb = iqldb;
    }

    public static class WaitingQueriesState {
        private List<SelectQuery> queries;

        public WaitingQueriesState(List<SelectQuery> queries) {
            this.queries = queries;
        }

        public List<SelectQuery> getQueries() {
            return queries;
        }

        public void setQueries(List<SelectQuery> queries) {
            this.queries = queries;
        }
    }

    public static class RunningQueriesState {
        private List<RunningQuery> queries;

        public RunningQueriesState(List<RunningQuery> queries) {
            this.queries = queries;
        }

        public List<RunningQuery> getQueries() {
            return queries;
        }

        public void setQueries(List<RunningQuery> queries) {
            this.queries = queries;
        }
    }

    @RequestMapping("/lastrunning")
    @ResponseBody
    public RunningQueriesState handleLastRunning() {
        return new RunningQueriesState(runningQueriesManager.lastDaemonRunningQueries);
    }

    @RequestMapping("/allrunning")
    @ResponseBody
    public RunningQueriesState handleAllRunning() {
        return new RunningQueriesState(iqldb.getRunningQueries());
    }

    @RequestMapping("/running")
    @ResponseBody
    public RunningQueriesState handleRunning() {
        return new RunningQueriesState(runningQueriesManager.getRunningReportForThisProcess());
    }

    @RequestMapping("/queue")
    @ResponseBody
    public RunningQueriesState handleQueue() {
        return new RunningQueriesState(runningQueriesManager.getWaitingReportForThisProcess());
    }

    @RequestMapping("/clearrunning")
    @ResponseBody
    public String handleClearRunning(@RequestParam(required = false) String hostname) {
        if(!Strings.isNullOrEmpty(hostname)) {
            int rowsDeleted = iqldb.clearRunningForHost(hostname);
            return "Deleted all " + rowsDeleted + " queries for host " + hostname + " from tblrunning";
        } else {
            int rowsDeleted = iqldb.clearRunningForThisHost();
            return "Deleted all " + rowsDeleted + " queries for this host from tblrunning";
        }
    }
}
