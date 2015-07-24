package com.indeed.squall.iql2.server.web;

import javax.annotation.Nonnull;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Sets header
 * Cache-control: no-cache, must-revalidate
 */
public class NoCacheFilter implements Filter {

    @Override
    public void init(@Nonnull final FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(@Nonnull final ServletRequest servletRequest,
                         @Nonnull final ServletResponse servletResponse,
                         @Nonnull final FilterChain filterChain) throws IOException, ServletException {
        if (servletRequest instanceof HttpServletRequest && servletResponse instanceof HttpServletResponse) {
            final HttpServletResponse response = (HttpServletResponse) servletResponse;
            response.setHeader("Cache-control", "no-cache, must-revalidate");
        }

        filterChain.doFilter(servletRequest, servletResponse);
    }

    /**
     * Noop
     */
    @Override
    public void destroy() {
        // noop
    }
}