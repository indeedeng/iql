package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.server.print.PrettyPrint;
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
    @RequestMapping("upgrade")
    @ResponseBody
    public Object upgrade(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final @Nonnull @RequestParam("q") String q
    ) {
        response.setHeader("Content-Type", "application/json");

        try {
            return ImmutableMap.of("upgraded", PrettyPrint.prettyPrint(q, true));
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
