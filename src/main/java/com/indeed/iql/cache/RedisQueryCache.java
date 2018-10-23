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

import com.google.common.io.ByteStreams;
import com.google.common.net.HostAndPort;
import org.apache.log4j.Logger;
import org.springframework.core.env.PropertyResolver;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author vladimir
 */

public class RedisQueryCache implements QueryCache {
    private static final Logger log = Logger.getLogger(RedisQueryCache.class);
    // redis client
    private final JedisCluster redis;

    public RedisQueryCache(PropertyResolver props) {
        final String redisHosts = props.getProperty("query.cache.redis.hosts");
        Set<redis.clients.jedis.HostAndPort> hostAndPorts = Arrays.stream(redisHosts.split(","))
                .map(HostAndPort::fromString).map(hostAndPort -> new redis.clients.jedis.HostAndPort(hostAndPort.getHostText(), hostAndPort.getPort())).collect(Collectors.toSet());
        final String redisPassword = props.getProperty("query.cache.redis.password");
        final int redisMaxIdleConnections = props.getProperty("query.cache.redis.max.idle.connections", Integer.class, 1);
        final int redisMaxTotalConnections = props.getProperty("query.cache.redis.max.total.connections", Integer.class, 20);
        final int redisMaxAttempts = props.getProperty("query.cache.redis.max.attempts", Integer.class, 2);
        final int redisTimeout = props.getProperty("query.cache.redis.timeout.ms", Integer.class, 20000);

        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(redisMaxTotalConnections); // maximum active connections
        poolConfig.setMaxIdle(redisMaxIdleConnections);  // maximum idle connections
        redis = new JedisCluster(hostAndPorts, redisTimeout, redisTimeout, redisMaxAttempts, redisPassword, poolConfig);
    }

    /**
     * Returns whether the Redis cache is available.
     */
    @Override
    public boolean isEnabled() {
        return true;
    }

    /**
     * Returns whether the cache was requested to be enabled in the app config.
     */
    @Override
    public boolean isEnabledInConfig() {
        return true;
    }

    @Override
    public boolean isFileCached(String fileName) {
        try {
            return redis.exists(fileName);
        } catch (Exception e) {
            return false;
        }

    }

    @Override
    @Nullable
    public InputStream getInputStream(String cachedFileName) {
        try {
            final byte[] content = redis.get(cachedFileName.getBytes());
            if(content == null) {
                return null; // not cached
            }
            return new ByteArrayInputStream(content);
        } catch (Exception e) {
            log.error("Error while reading from Redis cache", e);
            return null;
        }
    }

    @Override
    public OutputStream getOutputStream(String cachedFileName) {
        final ByteArrayOutputStream inMemoryCacheStream = new ByteArrayOutputStream(1000);
        
        return new OutputStream() {
            private boolean closed = false;

            @Override
            public void write(byte[] b) throws IOException {
                inMemoryCacheStream.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) {
                inMemoryCacheStream.write(b, off, len);
            }

            @Override
            public void flush() throws IOException {
                inMemoryCacheStream.flush();
            }

            @Override
            public void write(int b) {
                inMemoryCacheStream.write(b);
            }

            @Override
            public void close() throws IOException {
                if(closed) {
                    return;
                }
                closed = true;
                try {
                    redis.set(cachedFileName.getBytes(), inMemoryCacheStream.toByteArray());
                } finally {
                    inMemoryCacheStream.close();
                }
            }
        };
    }

    @Override
    public void writeFromFile(String cachedFileName, File localFile) throws IOException {
        final OutputStream cacheStream = getOutputStream(cachedFileName);
        try (final InputStream fileIn = new BufferedInputStream(new FileInputStream(localFile))) {
            ByteStreams.copy(fileIn, cacheStream);
        }
        // only close on success
        cacheStream.close();
    }

    /**
     * Tries to see if the Redis is accessible by checking if the root cache dir exists and recreating if necessary.
     * @throws IOException
     */
    @Override
    public void healthcheck() {
        if (!"test".equals(redis.echo("test"))) {
            throw new RuntimeException("redis cache test failed");
        }
    }
}
