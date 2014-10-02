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
