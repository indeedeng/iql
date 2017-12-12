package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.base.Strings;
import com.indeed.squall.iql2.server.web.model.IQLDB;
import com.indeed.squall.iql2.server.web.model.RunningQueriesManager;
import com.indeed.squall.iql2.server.web.model.RunningQuery;
import com.indeed.squall.iql2.server.web.model.SelectQuery;
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

