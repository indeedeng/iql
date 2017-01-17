/*
 * Copyright (C) 2017 Indeed Inc.
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
package com.indeed.imhotep.shortlink;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.codec.net.URLCodec;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.View;
import org.springframework.web.servlet.view.RedirectView;
import org.springframework.web.util.UriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Handles the HTTP requests for creating or retrieving short links
 */
@Controller
public class ShortLinkController {
    private static final Logger log = Logger.getLogger(ShortLinkController.class);

    private final ShortLinkRepository shortLinkRepository;

    @Autowired
    public ShortLinkController(final ShortLinkRepository shortLinkRepository) {
        this.shortLinkRepository = shortLinkRepository;
        log.info("Short linking enabled? " + shortLinkRepository.isEnabled());
    }

    @RequestMapping(value="/shortlink", method={RequestMethod.GET, RequestMethod.POST})
    @ResponseBody
    public Object create(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final UriComponentsBuilder uriBuilder,
            @RequestParam("q") final String query,
            @RequestParam(value = "view", defaultValue = "table") final String view) {

        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET, POST");
        response.setHeader("Access-Control-Max-Age", "3600");
        response.setHeader("Access-Control-Allow-Headers", "x-requested-with");

        String shortCode = null;
        for (int retries = 50; retries > 0; retries--) {
            try {
                final String allowed = "acdefghkmnprtwxyz2346789";
                final String attempt = RandomStringUtils.random(6, allowed).toUpperCase();
                if (shortLinkRepository.mapShortCode(attempt, query, view)) {
                    shortCode = attempt;
                    break;
                }
            } catch(IOException e) {
                if (retries == 50) {
                    log.warn("First attempt to map short code failed", e);
                }
            }
        }

        if (shortCode == null) {
            log.error("Failed to map a short code for " + query);
            throw new RuntimeException("Failed to create shortcode for query");
        }


        return ImmutableMap.of(
                "status", "ok",
                "url", uriBuilder.path("/q/"+shortCode).build());

    }

    @RequestMapping(value="/q/{shortcode}", method=RequestMethod.GET)
    public View redirect(@PathVariable("shortcode") final String shortCode) {
        try {
            final String resolved = shortLinkRepository.resolveShortCode(shortCode);
            log.info(shortCode + " resolved to " + resolved);
            final String[] viewAndQuery = resolved.split("\\|", 2);
            final String view = viewAndQuery[0];
            final String query = viewAndQuery[1];
            final URLCodec codec = new URLCodec("UTF-8");
            final String queryEncoded = codec.encode(query);
            return new RedirectView("/iql/#q[]=" + queryEncoded + "&view=" + view);
        } catch (Exception e) {
            log.error("Failed to handle /q/" + shortCode, e);
            return new RedirectView("/iql/");
        }
    }
}
