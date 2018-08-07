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

import org.apache.commons.codec.binary.Base64;

import javax.servlet.http.HttpServletRequest;

public class UsernameUtil {
    /**
     * Gets the user name from the HTTP request if it was provided through Basic authentication.
     *
     * @param request Http request
     * @return User name if Basic auth is used or null otherwise
     */
    public static String getUserNameFromRequest(final HttpServletRequest request) {
        final String authHeader = request.getHeader("Authorization");
        if (authHeader == null) {
            // try simple
            final String rawUser = request.getRemoteUser();
            if (rawUser == null) {
                return null;
            } else {
                return rawUser;
            }
        } else {
            final String credStr;
            if (authHeader.startsWith("user ")) {
                credStr = authHeader.substring(5);
            } else {
                // try basic auth
                if (!authHeader.toUpperCase().startsWith("BASIC ")) {
                    // Not basic
                    return null;
                }

                // remove basic
                final String credEncoded = authHeader.substring(6); //length of 'BASIC '

                final byte[] credRaw = Base64.decodeBase64(credEncoded.getBytes());
                if (credRaw == null) {
                    // invalid decoding
                    return null;
                }

                credStr = new String(credRaw);
            }

            // get username part from username:password
            final String[] x = credStr.split(":");
            if (x.length < 1) {
                // bad split
                return null;
            }

            return x[0];
        }
    }
}
