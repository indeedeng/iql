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
 package com.indeed.iql.web.config;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.ims.client.ImsClient;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.ims.server.SpringContextAware;
import com.indeed.iql.LocalImhotepDaemonAndShardmaster;
import com.indeed.iql.cache.QueryCache;
import com.indeed.iql.cache.QueryCacheFactory;
import com.indeed.iql.cache.RedisHostsOverride;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.AccessControl;
import com.indeed.iql.web.CORSInterceptor;
import com.indeed.iql.web.DataSourceLoader;
import com.indeed.iql.web.FieldFrequencyCache;
import com.indeed.iql.web.IQLDB;
import com.indeed.iql.web.ImhotepClientPinger;
import com.indeed.iql.web.Limits;
import com.indeed.iql.web.QueryServlet;
import com.indeed.iql.web.RunningQueriesManager;
import com.indeed.iql.web.SelectQuery;
import com.indeed.iql.web.TopTermsCache;
import com.indeed.iql1.sql.parser.SelectStatementParser;
import com.indeed.iql1.web.SplitterServlet;
import com.indeed.iql2.IQL2Options;
import com.indeed.iql2.server.web.servlets.ServletsPackageMarker;
import com.indeed.iql2.server.web.servlets.SplitServlet;
import com.indeed.iql2.server.web.servlets.query.QueryServletPackageMarker;
import com.indeed.util.core.threads.NamedThreadFactory;
import com.indeed.util.core.time.DefaultWallClock;
import com.indeed.util.core.time.WallClock;
import javafx.util.Pair;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertyResolver;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.servlet.config.annotation.DefaultServletHandlerConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import javax.annotation.PostConstruct;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.Query;
import javax.servlet.ServletContext;
import javax.sql.DataSource;
import javax.xml.bind.PropertyException;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableWebMvc
@EnableScheduling
@ComponentScan(basePackageClasses = {SpringConfiguration.class, QueryServlet.class,
        ServletsPackageMarker.class, QueryServletPackageMarker.class, SplitterServlet.class, SplitServlet.class})
public class SpringConfiguration extends WebMvcConfigurerAdapter {
    private static final Logger log = Logger.getLogger(SpringConfiguration.class);

    @Autowired
    ServletContext servletContext;

    @Autowired
    Environment env;

    @Bean
    SpringContextAware springContextAware(){
        return new SpringContextAware();
    }
    @Bean(destroyMethod = "shutdown")
    public ExecutorService cacheUploadExecutorService()  {
        return new ThreadPoolExecutor(
                3, 20, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                new NamedThreadFactory("IQL-Cache-Uploader")
        );
    }

    @Bean
    @Autowired
    MetricStatsEmitter metricStatsEmitter(
            MetricStatsEmitter metricStatsEmitter
    ) {
        if (metricStatsEmitter != null) {
            return metricStatsEmitter;
        } else {
            return MetricStatsEmitter.NULL_EMITTER;
        }
    }

    @Bean
    QueryCache queryCache(@Autowired(required = false) RedisHostsOverride redisHostsOverride) throws PropertyException {
        final Byte versionForHashing = env.getProperty("query.cache.version", Byte.class);
        if(versionForHashing != null) {
            SelectQuery.VERSION_FOR_HASHING += 5347 * versionForHashing;
        }
        final PropertyResolver propertyResolver;
        if(redisHostsOverride != null) {
            propertyResolver = redisHostsOverride.applyOverride(env);
        } else {
            propertyResolver = env;
        }

        return QueryCacheFactory.newQueryCache(propertyResolver);
    }

    @Bean
    FieldFrequencyCache fieldFrequencyCache() {
        @SuppressWarnings("unchecked")
        final List<String> allowedClients = (List<String>)env.getProperty("field.frequency.cache.allowed.clients", List.class, Collections.emptyList());
        return new FieldFrequencyCache(iqldb(), new HashSet<>(allowedClients));
    }

    @Bean(destroyMethod = "close")
    public ImhotepClient imhotepClient() {
        if(env.getProperty("imhotep.daemons.localmode", Boolean.class, false)) {
            // when running an imhotep daemon instance in process
            final String shardsDir = env.getProperty("imhotep.shards.directory");
            if(!Strings.isNullOrEmpty(shardsDir) && new File(shardsDir).exists()) {
                // TODO: make this also refresh a LocalShardMaster inside LocalImhotepDaemon
                final Pair<Integer, Integer> localPorts = LocalImhotepDaemonAndShardmaster.startInProcess(shardsDir);
                final ImhotepClient client = getImhotepClient("", "", "localhost:" + localPorts.getKey());
                client.setImhotepDaemonsOverride(env.getProperty("imhotep.daemons.override"));
                return client;
            } else {
                log.warn("Local mode is enabled for the Imhotep Daemon but imhotep.shards.directory is not set to an existing location." +
                        "It should be set to the local path of a directory containing the Imhotep indexes and shards to be served.");
            }
        }
        // connect to an externally running ShardMaster via its leader election node
        ImhotepClient client = getImhotepClient(
                env.getProperty("imhotep.shardmaster.zookeeper.quorum"),
                env.getProperty("imhotep.shardmaster.zookeeper.path"),
                env.getProperty("imhotep.shardmaster.host"));
        client.setImhotepDaemonsOverride(env.getProperty("imhotep.daemons.override"));
        return client;
    }

    private ImhotepClient getImhotepClient(String zkNodes, String zkPath, String hosts) {
        if(!Strings.isNullOrEmpty(hosts)) {
            final List<Host> hostObjects = Lists.newArrayList();
            for(String host : hosts.split(",")) {
                String[] hostParts = host.split(":");
                hostObjects.add(new Host(hostParts[0], Integer.parseInt(hostParts[1])));
            }
            return new ImhotepClient(hostObjects);
        } else if(!Strings.isNullOrEmpty(zkNodes)) {
            return new ImhotepClient(zkNodes, zkPath, true);
        } else {
            throw new IllegalArgumentException("either imhotep.shardmaster.zookeeper.quorum or imhotep.shardmaster.host config properties must be set");
        }
    }

    // TODO: merge!
    // IQL1 metadata cache
    @Bean
    public ImhotepMetadataCache metadataCacheIQL1() {
        return new ImhotepMetadataCache(imsClientIQL1(), imhotepClient(), env.getProperty("disabled.fields"), fieldFrequencyCache(), false);
    }
    // IQL2 metadata cache
    @Bean
    public ImhotepMetadataCache metadataCacheIQL2() {
        return new ImhotepMetadataCache(imsClientIQL2(), imhotepClient(), env.getProperty("disabled.fields"), fieldFrequencyCache(), true);
    }
    @Bean
    public TopTermsCache topTermsCache() {
        final boolean topTermsCacheEnabled = env.getProperty("topterms.cache.enabled", Boolean.class, true);
        if(!topTermsCacheEnabled) {
            log.info("TopTermsCache disabled by the topterms.cache.enabled setting");
        }
        return new TopTermsCache(imhotepClient(), env.getProperty("topterms.cache.dir"),
                IQLEnv.fromSpring(env) == IQLEnv.DEVELOPER, topTermsCacheEnabled);
    }

    @Bean
    public IQLEnv iqlEnv() {
        return IQLEnv.fromSpring(env);
    }

    // We need 2 instances to be able to use them concurrently
    @Bean(name = "imsClientIQL1")
    public ImsClientInterface imsClientIQL1() {
        return createIMSClient();
    }

    @Bean(name = "imsClientIQL2")
    public ImsClientInterface imsClientIQL2() {
        return createIMSClient();
    }

    private ImsClientInterface createIMSClient() {
        final boolean imsEnabled = env.getProperty("ims.enabled", Boolean.class, true);
        try {
            ///A way to get the port from tomcat without a request
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            Set<ObjectName> objs = mbs.queryNames(new ObjectName("*:type=Connector,*"),
                    Query.match(Query.attr("protocol"), Query.value("HTTP/1.1")));
            ArrayList<String> ports = new ArrayList<String>();
            for (ObjectName obj : objs) {
                String port = obj.getKeyProperty("port");
                ports.add(port);
            }
            String url = "http://localhost:" + ports.get(0) + servletContext.getContextPath() + "/";
            if(imsEnabled) {
                return ImsClient.build(url);
            } else {
                log.info("IMS disabled by the ims.enabled setting");
                return null;
            }
        } catch (URISyntaxException e) {
            log.error("Failed to connect to the metadata service",e);
        }
        catch (Exception e) {
            log.error(e);
        }
        return null;
    }

    @Bean
    public Integer rowLimit() {
        return env.getProperty("row.limit", Integer.class, 1000000);
    }

    @Bean
    public Long maxCachedQuerySizeLimitBytes() {
        final long limitInMegabytes = env.getProperty("iql.max.cached.query.size.mb.limit", Long.class, Long.MAX_VALUE);
        if (limitInMegabytes < Long.MAX_VALUE) {
            return mbToBytes(limitInMegabytes);
        } else {
            return null;
        }
    }

    @Bean
    public Long imhotepLocalTempFileSizeLimit() {
        final long limitInMegabytes = env.getProperty("imhotep.local.temp.file.size.mb.limit", Long.class, Long.MAX_VALUE);
        if(limitInMegabytes < Long.MAX_VALUE) {
            return mbToBytes(limitInMegabytes);
        } else {
            return Long.MAX_VALUE;
        }
    }

    @Bean
    public Long imhotepDaemonTempFileSizeLimit() {
        return mbToBytes(env.getProperty("imhotep.daemon.temp.file.size.mb.limit", Long.class, -1L));
    }

    private Long mbToBytes(Long megabytes) {
        return megabytes <= 0 ? megabytes : megabytes * 1024 * 1024;
    }

    @Bean
    public Long docCountLimit() {
        return env.getProperty("query.document.count.limit", Long.class, 0L);
    }

    @Bean
    public AccessControl accessControl() {
        @SuppressWarnings("unchecked")
        final List<String> bannedUserList = (List<String>)env.getProperty("banned.users", List.class, Collections.emptyList());
        @SuppressWarnings("unchecked")
        final List<String> multiuserClients = (List<String>)env.getProperty("multiuser.clients", List.class, Collections.emptyList());
        @SuppressWarnings("unchecked")
        final Set<String> privilegedDatasets = (Set<String>)env.getProperty("privileged.datasets", Set.class, Collections.emptySet());
        @SuppressWarnings("unchecked")
        final Set<String> privilegedDatasetsUsers = (Set<String>)env.getProperty("privileged.datasets.users", Set.class, Collections.emptySet());

        return new AccessControl(bannedUserList, multiuserClients, iqldb(), getDefaultLimits(), privilegedDatasets, privilegedDatasetsUsers);
    }

    private Limits getDefaultLimits() {
        final Long queryDocumentCountLimit = env.getProperty("query.document.count.limit", Long.class);
        return new Limits(
                queryDocumentCountLimit != null ? (int) (queryDocumentCountLimit / 1_000_000_000) : null,
                env.getProperty("row.limit", Integer.class),
                env.getProperty("imhotep.local.temp.file.size.mb.limit", Integer.class),
                env.getProperty("imhotep.daemon.temp.file.size.mb.limit", Integer.class),
                env.getProperty("user.concurrent.query.limit", Integer.class),
                env.getProperty("user.concurrent.imhotep.sessions.limit", Integer.class)

        );
    }

    @Bean
    public IQL2Options defaultIQLOptions() {
        return new IQL2Options();
    }

    @Bean
    public IQLDB iqldb() {
        final DataSource dataSource = iqlDbDataSource();
        return dataSource != null ? new IQLDB(dataSource) : null;
    }

    @Bean
    public DataSource iqlDbDataSource() {
        return DataSourceLoader.tryGetDataSource("iqldb", env);
    }

    @Bean
    public int perUserPendingQueriesLimit() {
        return env.getProperty("iql.per.user.pending.queries.limit", Integer.class, Integer.MAX_VALUE);
    }

    @Bean
    public RunningQueriesManager runningQueriesManager() {
        final RunningQueriesManager runningQueriesManager = new RunningQueriesManager(iqldb(), perUserPendingQueriesLimit());
        runningQueriesManager.onStartup();
        return runningQueriesManager;
    }

    @Bean
    public ImhotepClientPinger imhotepClientPinger() {
        return new ImhotepClientPinger(imhotepClient());
    }

    public static @Bean
    PropertySourcesPlaceholderConfigurer placeholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Override
    public void configureDefaultServletHandling(DefaultServletHandlerConfigurer configurer) {
        configurer.enable();
    }

    @Bean
    CORSInterceptor corsInterceptor() {
        return new CORSInterceptor(env);
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(corsInterceptor());
    }

    @PostConstruct
    public void init() {
        SelectStatementParser.LOWEST_YEAR_ALLOWED = env.getProperty("lowest.year.allowed", Integer.class, 0);
    }

    // Serve IMS statics
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/metadata/**").addResourceLocations("classpath:/META-INF/public-web-resources/metadata/");
    }

    // Serve IMS index.html when directory is requested
    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/metadata").setViewName("redirect:/metadata/");
        registry.addViewController("/metadata/").setViewName("forward:/metadata/index.html");
    }

    // Configure default JSON serializer to produce pretty/indented output for human readability
    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
        builder.indentOutput(true);
        converters.add(new MappingJackson2HttpMessageConverter(builder.build()));
        converters.add(new StringHttpMessageConverter(Charsets.UTF_8));
    }

    // do we need this?
//    @PreDestroy
//    public void destroy() throws IOException {
//        try {
//            cacheUploadExecutorService().awaitTermination(60, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            throw new IOException("interrupted while shutting down", e);
//        }
//    }

    // IQL2 beans
    @Bean
    WallClock clock() {
        return new DefaultWallClock();
    }

    // Used for running @Schedule tasks
    @Bean
    public ScheduledThreadPoolExecutor scheduledThreadPoolExecutor() {
        return new ScheduledThreadPoolExecutor(4);
    }

    @Bean
    public File tmpDir() {
        return null;
    }

    @Bean
    public Integer maxPostSize() {
        // Should be configured to match the container context
        // If unset, users are likely to get cryptic errors such as
        // "Required String parameter 'q' is not present"
        return env.getProperty("max.post.size", Integer.class, Integer.MAX_VALUE);
    }
}
