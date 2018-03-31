package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.base.Strings;
import com.indeed.common.net.GoogleShortlinkClient;
import com.indeed.common.util.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author vladimir
 */
@Controller
public class ShortlinkController {
    private static final Logger log = Logger.getLogger(ShortlinkController.class);

    private static final int MAX_ATTEMPTS = 3;
    private static final String DEFAULT_LINK_PREFIX = "SQ";

    @RequestMapping(value = "/shortlink", produces = "application/json")
    @ResponseBody
    public String doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final String url = request.getParameter("url");
        String linkPrefix = request.getParameter("prefix");
        if(Strings.isNullOrEmpty(linkPrefix)) {
            linkPrefix = DEFAULT_LINK_PREFIX;
        }

        // allow origins within Indeed
        // now could be handled by CORSInterceptor but we also set 'Access-Control-Allow-Credentials' here
        final String origin = request.getHeader("Origin");
        if(origin != null) {
            try {
                final String originHost = new URI(origin).getHost();
                if(originHost.endsWith("indeed.com") || originHost.endsWith("indeed.net")){
                    response.setHeader("Access-Control-Allow-Origin", origin);
                    response.setHeader("Access-Control-Allow-Credentials", "true");
                }
            } catch (URISyntaxException ignored) {
            }
        }

        // pretend it's JSON
        return '"' + createShortlink(url, linkPrefix) + '"';
    }

    private String createShortlink(String url, String linkPrefix) {
        GoogleShortlinkClient gslc = new GoogleShortlinkClient();
        String shortlink = "";
        if (!StringUtils.isEmpty(url)) {
            int attemptsLeft = MAX_ATTEMPTS;
            while (attemptsLeft > 0) {
                try {
                    shortlink = gslc.createLinkWithPrefix(linkPrefix, url, null, true);
                    break;
                } catch (Exception e) {
                    attemptsLeft--;
                    if(attemptsLeft == 0) {
                        log.warn("failed to create shortlink", e);
                    }
                }
            }
        }
        return shortlink;
    }
}
