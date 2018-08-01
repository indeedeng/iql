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

package com.indeed.iql.web;

import com.indeed.imhotep.exceptions.ImhotepKnownException;
import com.indeed.iql.exceptions.IqlKnownException;
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
        final int defaultLanguageVersion = 1;
        return ServletRequestUtils.getIntParameter(request, "v", defaultLanguageVersion) == 1 ? 1 : 2;
    }

    public static boolean isKnownError(final Throwable error) {
        return (error instanceof ImhotepKnownException) || (error instanceof IqlKnownException);
    }
}
