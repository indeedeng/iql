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

import java.util.List;


/**
 * @author vladimir
 */
@Controller
public class RunningController {
    private final ExecutionManager executionManager;

    @Autowired
    public RunningController(ExecutionManager executionManager) {
        this.executionManager = executionManager;
    }

    @RequestMapping("/running")
    @ResponseBody
    public State handle() {
        return new State(executionManager.getRunningQueries());
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
}
