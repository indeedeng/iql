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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;


/**
 * @author vladimir
 */
@Controller
public class RunningController {
    private final ExecutionManager executionManager;
    private final RunningQueriesManager runningQueriesManager;
    private final IQLDB iqldb;

    @Autowired
    public RunningController(ExecutionManager executionManager, RunningQueriesManager runningQueriesManager, IQLDB iqldb) {
        this.executionManager = executionManager;
        this.runningQueriesManager = runningQueriesManager;
        this.iqldb = iqldb;
    }

    @RequestMapping("/running")
    @ResponseBody
    public State handleRunning() {
        List<ExecutionManager.QueryTracker> queries = executionManager.getRunningQueries();
        Collections.sort(queries, new Comparator<ExecutionManager.QueryTracker>() {
            @Override
            public int compare(ExecutionManager.QueryTracker o1, ExecutionManager.QueryTracker o2) {
                return o1.getStartedTime().compareTo(o2.getStartedTime());
            }
        });
        return new State(queries);
    }

    public static class State {
        private List<ExecutionManager.QueryTracker> queries;

        public State(List<ExecutionManager.QueryTracker> queries) {
            this.queries = queries;
        }

        public List<ExecutionManager.QueryTracker> getQueries() {
            return queries;
        }

        public void setQueries(List<ExecutionManager.QueryTracker> queries) {
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
}
