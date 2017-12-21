/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.web.config;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.indeed.imhotep.LocalImhotepDaemon;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.iql.cache.QueryCache;
import com.indeed.imhotep.iql.cache.QueryCacheFactory;
import com.indeed.imhotep.sql.parser.StatementParser;
import com.indeed.imhotep.web.AccessControl;
import com.indeed.imhotep.web.CORSInterceptor;
import com.indeed.imhotep.web.DataSourceLoader;
import com.indeed.imhotep.web.IQLDB;
import com.indeed.imhotep.web.ImhotepClientPinger;
import com.indeed.imhotep.web.ImhotepMetadataCache;
import com.indeed.imhotep.web.Limits;
import com.indeed.imhotep.web.QueryServlet;
import com.indeed.imhotep.web.RunningQueriesManager;
import com.indeed.imhotep.web.TopTermsCache;
import com.indeed.ims.server.SpringContextAware;
import com.indeed.util.core.threads.NamedThreadFactory;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;
import org.springframework.http.converter.HttpMessageConverter;
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
import javax.sql.DataSource;
import javax.xml.bind.PropertyException;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableWebMvc
@EnableScheduling
@ComponentScan(basePackageClasses = {SpringConfiguration.class,QueryServlet.class})
public class SpringConfiguration extends WebMvcConfigurerAdapter {
    private static final Logger log = Logger.getLogger(SpringConfiguration.class);

    @Autowired
    Environment env;

    @Bean
    SpringContextAware springContextAware(){
        return new SpringContextAware();
    }
    @Bean(destroyMethod = "shutdown")
    public ExecutorService executorService()  {
        return new ThreadPoolExecutor(
                3, 10, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(100),
                new NamedThreadFactory("IQL-Worker")
        );
    }

    @Bean 
    QueryCache queryCache() throws PropertyException {
        return QueryCacheFactory.newQueryCache(env);
    }

    @Bean(destroyMethod = "close")
    public ImhotepClient imhotepClient() {
        if(env.getProperty("imhotep.daemons.localmode", Boolean.class, false)) {
            // when running an imhotep daemon instance in process
            final String shardsDir = env.getProperty("imhotep.shards.directory");
            if(!Strings.isNullOrEmpty(shardsDir) && new File(shardsDir).exists()) {
                final int localImhotepPort = LocalImhotepDaemon.startInProcess(shardsDir);
                return getImhotepClient("", "", "localhost:" + localImhotepPort, false);
            } else {
                log.warn("Local mode is enabled for the Imhotep Daemon but imhotep.shards.directory is not set to an existing location." +
                        "It should be set to the local path of a directory containing the Imhotep indexes and shards to be served.");
            }
        }
        // connect to an externally running Imhotep Daemon
        return getImhotepClient(
                env.getProperty("imhotep.daemons.zookeeper.quorum"),
                env.getProperty("imhotep.daemons.zookeeper.path"),
                env.getProperty("imhotep.daemons.host"),
                false);
    }

    @Bean(destroyMethod = "close")
    public ImhotepClient imhotepInteractiveClient() {
        final ImhotepClient interactiveClient = getImhotepClient(
                env.getProperty("imhotep.daemons.interactive.zookeeper.quorum"),
                env.getProperty("imhotep.daemons.interactive.zookeeper.path"),
                env.getProperty("imhotep.daemons.interactive.host"),
                true);
        if(interactiveClient != null) {
            return interactiveClient;
        } else {
            // interactive not provided, reuse the normal client
            return imhotepClient();
        }
    }

    private ImhotepClient getImhotepClient(String zkNodes, String zkPath, String hosts, boolean quiet) {
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
            if(quiet) {
                return null;
            }
            throw new IllegalArgumentException("either imhotep.daemons.zookeeper.quorum or imhotep.daemons.host config properties must be set");
        }
    }

    @Bean
    public ImhotepMetadataCache metadataCache() {
        return new ImhotepMetadataCache(imhotepClient(), env.getProperty("disabled.fields"),
                env.getProperty("ims.enabled", Boolean.class, true));
    }

    @Bean
    public TopTermsCache topTermsCache() {
        return new TopTermsCache(imhotepClient(), metadataCache(), env.getProperty("topterms.cache.dir"),
                IQLEnv.fromSpring(env) == IQLEnv.DEVELOPER);
    }

    @Bean
    public Integer rowLimit() {
        return env.getProperty("row.limit", Integer.class, 1000000);
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

        return new AccessControl(bannedUserList, multiuserClients, iqldb(), getDefaultLimits());
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
    public IQLDB iqldb() {
        final DataSource dataSource = iqlDbDataSource();
        return dataSource != null ? new IQLDB(dataSource) : null;
    }

    @Bean
    public DataSource iqlDbDataSource() {
        return DataSourceLoader.tryGetDataSource("iqldb", env);
    }

    @Bean
    public RunningQueriesManager runningQueriesManager() {
        final RunningQueriesManager runningQueriesManager = new RunningQueriesManager(iqldb());
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
        StatementParser.LOWEST_YEAR_ALLOWED = env.getProperty("lowest.year.allowed", Integer.class, 0);
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
    }

    // do we need this?
//    @PreDestroy
//    public void destroy() throws IOException {
//        try {
//            executorService().awaitTermination(60, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            throw new IOException("interrupted while shutting down", e);
//        }
//    }
}
