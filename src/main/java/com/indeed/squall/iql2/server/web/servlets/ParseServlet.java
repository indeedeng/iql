package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.server.web.metadata.MetadataCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;

@Controller
public class ParseServlet {

    final MetadataCache metadataCache;

    @Autowired
    public ParseServlet(final MetadataCache metadataCache) {
        this.metadataCache = metadataCache;
    }

//    @RequestMapping("parse")
    @ResponseBody
    public Object parse(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final @Nonnull @RequestParam("q") String q
    ) {
        final int version = ServletUtil.getIQLVersionBasedOnParam(request);
        try {
            response.setHeader("Content-Type", "application/json");
            Queries.parseQuery(q, version == 1, metadataCache.get(), new StoppedClock());
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
