package com.indeed.squall.iql2.server.web.servlets;

import org.springframework.web.bind.ServletRequestUtils;

import javax.servlet.http.HttpServletRequest;

public class ServletUtil {
    public static int getIQLVersionBasedOnPath(HttpServletRequest request) {
        if(request.getRequestURI().startsWith("/iql2/")) {
            return 2;
        } else {
            return 1;
        }
    }

    public static int getIQLVersionBasedOnParam(HttpServletRequest request) {
        final int fallbackVersion = getIQLVersionBasedOnPath(request);
        return ServletRequestUtils.getIntParameter(request, "v", fallbackVersion);
    }
}
