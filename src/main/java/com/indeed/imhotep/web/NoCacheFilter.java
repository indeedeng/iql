/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.web;

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
