package com.indeed.squall.iql2.server.web.servlets;

import org.springframework.web.bind.ServletRequestUtils;

import javax.servlet.http.HttpServletRequest;

public class ServletUtil {
    public static int getVersion(HttpServletRequest request) {
        final int fallbackVersion;
        if (request.getServletPath().startsWith("/iql2/")) {
            fallbackVersion = 2;
        } else {
            fallbackVersion = 1;
        }
        return ServletRequestUtils.getIntParameter(request, "v", fallbackVersion);
    }
}
