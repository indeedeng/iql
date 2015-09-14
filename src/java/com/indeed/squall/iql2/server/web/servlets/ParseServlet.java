package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.query.Query;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Controller
public class ParseServlet {

    // TODO: use a shared reloader, and have actual values
    private Map<String, Set<String>> getKeywordAnalyzerWhitelist() {
        return Collections.emptyMap();
    }

    // TODO: use a shared reloader, and have actual values
    private Map<String, Set<String>> getDatasetToIntFields() {
        return Collections.emptyMap();
    }

    @RequestMapping("parse")
    @ResponseBody
    public Object parse(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final @Nonnull @RequestParam("q") String q
    ) {
        final int version = ServletUtil.getVersion(request);
        try {
            response.setHeader("Content-Type", "application/json");
            final Query query = Queries.parseQuery(q, version == 1, getKeywordAnalyzerWhitelist(), getDatasetToIntFields());
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
