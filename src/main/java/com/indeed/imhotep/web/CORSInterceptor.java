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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.springframework.core.env.Environment;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * @author vladimir
 */

public class CORSInterceptor extends HandlerInterceptorAdapter {
    private static final Logger log = Logger.getLogger(CORSInterceptor.class);


    private List<Pattern> allowedHostRegexes = Lists.newArrayList();

    public CORSInterceptor(Environment env) {
        //noinspection unchecked
        final List<String> allowedHostMasks = (List<String>)env.getProperty("cors.allowed.hosts", List.class, Lists.newArrayList());
        for(String hostMask : allowedHostMasks) {
            if(hostMask.isEmpty()) {
                continue; // empty means no CORS
            }
            try {
                final Pattern hostRegex = Pattern.compile(hostMask);
                allowedHostRegexes.add(hostRegex);
            } catch (PatternSyntaxException e) {
                log.error("Failed to parse a regex provided in cors.allowed.hosts config property: " + hostMask, e);
            }
        }
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        final String origin = request.getHeader("Origin");
        if(!Strings.isNullOrEmpty(origin) && !request.getServletPath().contains("\\private\\")) {
            for (Pattern allowedHostRegex : allowedHostRegexes) {
                if (allowedHostRegex.matcher(origin).matches()) {
                    response.setHeader("Access-Control-Allow-Origin", origin);
                    break;
                }
            }
        }
        return true;
    }
}
