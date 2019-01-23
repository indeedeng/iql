package com.indeed.iql.web.filters;

import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * The purpose of this filter is to give better error messages than Tomcat.
 * <p>
 * When Tomcat receives a POST body that is too large, it silently swallows it
 * and tries to continue with the query parameters that have been parsed up until that
 * point.
 * <p>
 * This results in cryptic messages when the corresponding servlet searches for the 'q' parameter,
 * finds it missing, and tells the user that they need to set 'q', even though it was potentially
 * present in the request body.
 */
public class LargePostFilter implements Filter {
    private static final Logger log = Logger.getLogger(LargePostFilter.class);

    private Integer maxPostSize = null;
    private boolean initialized = false;

    @Override
    public void init(final FilterConfig filterConfig) {
    }

    @Override
    public void doFilter(
            final ServletRequest servletRequest,
            final ServletResponse servletResponse,
            final FilterChain filterChain) throws IOException, ServletException {
        // filters cannot be beans, so we lazily instantiate on first call
        if (!initialized) {
            // Set to initialized first so we don't keep trying if this causes errors somehow
            initialized = true;
            final ServletContext servletContext = servletRequest.getServletContext();
            // getWebApplicationContext fails, so we use findWebApplicationContext and hope it all works out?
            final WebApplicationContext webApplicationContext = WebApplicationContextUtils.findWebApplicationContext(servletContext);
            try {
                maxPostSize = webApplicationContext.getBean("maxPostSize", Integer.class);
            } catch (final BeansException e) {
                log.error("Could not resolve maxPostSize bean. Running with LargePostFilter inactive.", e);
            }
        }

        if ((maxPostSize != null) && (servletRequest.getContentLength() > maxPostSize) && (servletResponse instanceof HttpServletResponse)) {
            final HttpServletResponse resp = (HttpServletResponse) servletResponse;
            resp.sendError(413, "POST request size (" + servletRequest.getContentLength() + ") larger than max post size (" + maxPostSize + ")");
            return;
        }

        filterChain.doFilter(servletRequest, servletResponse);
    }

    @Override
    public void destroy() {
    }
}
