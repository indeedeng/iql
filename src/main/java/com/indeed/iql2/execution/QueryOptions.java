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

package com.indeed.iql2.execution;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.indeed.imhotep.client.Host;
import com.indeed.iql.exceptions.IqlKnownException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author aibragimov
 *
 * This is list of all supported options with description gathered in one place.
 */
public class QueryOptions {

    // Don't do cache lookup, force run query.
    public static final String NO_CACHE = "nocache";

    // Enable port forwarding to use rust daemon
    public static final String USE_RUST_DAEMON = "rust";

    // Fail after executing last command
    public static final String DIE_AT_END = "die_at_end";

    // Ensure that a bunch of (not free to check) invariants are met during query execution
    public static final String PARANOID = "paranoid";

    // Temporary features, now in test mode.
    // After testing should be deleted or moved to main features list.
    public static class Experimental {
        private static final Pattern HOSTS_MAPPING_METHOD_PATTERN = Pattern.compile("^hostsmappingmethod=(\\w|_)*$");
        private static final Pattern HOSTS_PATTERN = Pattern.compile("^hosts=\\[.*\\]$");

        private static final Splitter COMMA_SPLITTER = Splitter.on(",");
        private static final Splitter COLON_SPLITTER = Splitter.on(":");
        private static final Splitter EQUALITY_SPLITTER = Splitter.on("=");

        public static final String ASYNC = "async";

        public static final String BATCH = "batch";

        public static final String PWHERE = "where2";

        public static final String P2P_CACHE = "p2pcache";

        private Experimental() {
        }

        public static HostsMappingMethod parseHostMappingMethod(final Collection<String> queryOptions) {
            final Optional<String> mappingStr = queryOptions
                    .stream()
                    .filter(option -> HOSTS_MAPPING_METHOD_PATTERN.matcher(option.trim()).matches())
                    .findFirst();
            if (!mappingStr.isPresent()) {
                return HostsMappingMethod.getDefaultMethod();
            }

            final String[] methodParts = Iterables.toArray(EQUALITY_SPLITTER.split(mappingStr.get().trim()), String.class);
            if (methodParts.length != 2) {
                throw new IqlKnownException.OptionsErrorException("couldn't parse hostsmappingmethod option");
            }
            return HostsMappingMethod.fromString(methodParts[1]);
        }

        public static boolean hasHosts(final Collection<String> queryOptions) {
            return getHosts(queryOptions).isPresent();
        }

        public static List<Host> parseHosts(final Collection<String> queryOptions) {
            final Optional<String> hostsOption = getHosts(queryOptions);
            if (!hostsOption.isPresent()) {
                return Collections.emptyList();
            }

            final String hostListStr = hostsOption.get().trim().replaceAll("(^hosts=\\[)|(\\]$)", "");
            // Format of Hosts String: hosts=[hosts1,host2,...]
            try {
                return COMMA_SPLITTER
                        .splitToList(hostListStr)
                        .stream()
                        .map(hostStr -> {
                            final String[] hostParts = Iterables.toArray(COLON_SPLITTER.split(hostStr.trim()), String.class);
                            return new Host(hostParts[0], Integer.parseInt(hostParts[1]));
                        })
                        .collect(Collectors.toList());
            } catch (final Exception e) {
                throw new IqlKnownException.OptionsErrorException("couldn't parse the hosts string in options", e);
            }
        }

        private static Optional<String> getHosts(final Collection<String> queryOptions) {
            return queryOptions
                    .stream()
                    .filter(option -> HOSTS_PATTERN.matcher(option.trim()).matches())
                    .findFirst();
        }
    }

    public enum HostsMappingMethod {
        SHUFFLE_MAPPING("shuffle"), // the default method
        NUMDOC_MAPPING("num_doc");

        private final String value;

        HostsMappingMethod(final String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }

        public static HostsMappingMethod getDefaultMethod() {
            return HostsMappingMethod.SHUFFLE_MAPPING;
        }

        public static HostsMappingMethod fromString(final String text) {
            for (HostsMappingMethod method : HostsMappingMethod.values()) {
                if (method.value.equals(text)) {
                    return method;
                }
            }
            throw new IqlKnownException.OptionsErrorException("unknown host mapping method, method = " + text);
        }
    }

    public static boolean includeInCacheKey(final String option) {
        return false;
    }

    private QueryOptions() {
    }
}
