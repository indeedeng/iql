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

package com.indeed.iql.cache;

import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertyResolver;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertySourcesPropertyResolver;

import java.util.Collections;
import java.util.Map;

/**
 * When present as a bean on the Spring context it overrides the hosts used to connect to the Redis query cache
 */
public class RedisHostsOverride {
    private final String hosts;

    public RedisHostsOverride(String hosts) {
        this.hosts = hosts;
    }

    public PropertyResolver applyOverride(Environment env) {
        return overridePropertiesFromEnvironment(Collections.singletonMap("query.cache.redis.hosts", hosts), env);
    }

    private static PropertyResolver overridePropertiesFromEnvironment(Map<String, Object> propertiesToOverride, Environment env) {
        final MutablePropertySources newPropertySources = new MutablePropertySources(((AbstractEnvironment) env).getPropertySources());
        final PropertySource redisFromConsulPropertySource = new MapPropertySource("redisHostsOverride",
                propertiesToOverride);
        newPropertySources.addFirst(redisFromConsulPropertySource);
        return new PropertySourcesPropertyResolver(newPropertySources);
    }
}
