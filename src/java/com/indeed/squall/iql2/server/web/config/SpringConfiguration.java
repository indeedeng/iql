package com.indeed.squall.iql2.server.web.config;

import com.google.common.base.Strings;
import com.indeed.common.util.time.DefaultWallClock;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.ims.client.ImsClient;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.squall.common.web.DataSourceLoader;
import com.indeed.squall.iql2.server.LocalImhotepDaemon;
import com.indeed.squall.iql2.server.web.AccessControl;
import com.indeed.squall.iql2.server.web.CORSInterceptor;
import com.indeed.squall.iql2.server.web.WebPackageMarker;
import com.indeed.squall.iql2.server.web.cache.QueryCache;
import com.indeed.squall.iql2.server.web.cache.QueryCacheFactory;
import com.indeed.squall.iql2.server.web.healthcheck.HealthcheckPackageMarker;
import com.indeed.squall.iql2.server.web.healthcheck.ImhotepClientPinger;
import com.indeed.squall.iql2.server.web.healthcheck.ImhotepMetadataServiceClientPinger;
import com.indeed.squall.iql2.server.web.metadata.MetadataCache;
import com.indeed.squall.iql2.server.web.model.IQLDB;
import com.indeed.squall.iql2.server.web.model.Limits;
import com.indeed.squall.iql2.server.web.model.RunningQueriesManager;
import com.indeed.squall.iql2.server.web.servlets.ServletsPackageMarker;
import com.indeed.squall.iql2.server.web.servlets.query.QueryServletPackageMarker;
import com.indeed.squall.iql2.server.web.topterms.TopTermsCache;
import com.indeed.util.core.time.WallClock;
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
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import javax.sql.DataSource;
import javax.xml.bind.PropertyException;
import java.io.File;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@Configuration
@EnableWebMvc
@EnableScheduling
@ComponentScan(basePackageClasses = {ConfigurationPackageMarker.class, ServletsPackageMarker.class, QueryServletPackageMarker.class, WebPackageMarker.class, HealthcheckPackageMarker.class})
public class SpringConfiguration extends WebMvcConfigurerAdapter {
    private static final Logger log = Logger.getLogger(SpringConfiguration.class);

    @Autowired
    Environment env;

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

    @Bean
    public ImsClientInterface imsClient() {
        try {
            return ImsClient.build(env.getProperty("ims.url"));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("get ImhotepMetaDataService "+env.getProperty("ims.url") + " error: "+ e.getMessage());
        }
    }

//    @Bean(destroyMethod = "close")
//    public ImhotepClient imhotepInteractiveClient() {
//        final ImhotepClient interactiveClient = getImhotepClient(
//                env.getProperty("imhotep.daemons.interactive.zookeeper.quorum"),
//                env.getProperty("imhotep.daemons.interactive.zookeeper.path"),
//                env.getProperty("imhotep.daemons.interactive.host"),
//                true);
//        if(interactiveClient != null) {
//            return interactiveClient;
//        } else {
//            // interactive not provided, reuse the normal client
//            return imhotepClient();
//        }
//    }

    private ImhotepClient getImhotepClient(String zkNodes, String zkPath, String host, boolean quiet) {
        if(!Strings.isNullOrEmpty(host)) {
            String mergePoint = host.split(",")[0];
            String[] mergePointParts = mergePoint.split(":");
            List<Host> hosts = Arrays.asList(new Host(mergePointParts[0], Integer.parseInt(mergePointParts[1])));
            return new ImhotepClient(hosts);
        } else if(!Strings.isNullOrEmpty(zkNodes)) {
            if (Strings.isNullOrEmpty(zkPath)) {
                return new ImhotepClient(zkNodes, true);
            } else {
                return new ImhotepClient(zkNodes, zkPath, true);
            }
        } else {
            if(quiet) {
                return null;
            }
            throw new IllegalArgumentException("either imhotep.daemons.zookeeper.quorum or imhotep.daemons.host config properties must be set");
        }
    }

    @Bean
    QueryCache queryCache() throws PropertyException {
        return QueryCacheFactory.newQueryCache(env);
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer placehodlerConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public ScheduledThreadPoolExecutor scheduledThreadPoolExecutor() {
        return new ScheduledThreadPoolExecutor(4);
    }

    @Bean
    public MetadataCache metadataCache(ImsClientInterface imsClient, ImhotepClient imhotepClient) {
        return new MetadataCache(imsClient, imhotepClient);
    }

    @Bean
    public TopTermsCache topTermsCache() {
        return new TopTermsCache(
                imhotepClient(),
                env.getProperty("topterms.cache.dir"),
                IQLEnv.fromSpring(env) == IQLEnv.DEVELOPER
        );
    }

    @Bean
    public DataSource iqlDbDataSource() {
        return DataSourceLoader.tryGetDataSource("iqldb", env);
    }

    @Bean
    public IQLDB iqldb() {
        final DataSource dataSource = iqlDbDataSource();
        if (dataSource == null) {
            return null;
        }
        return new IQLDB(dataSource);
    }

    @Bean
    public AccessControl accessControl() {
        @SuppressWarnings("unchecked")
        final List<String> bannedUserList = (List<String>)env.getProperty("banned.users", List.class, Collections.emptyList());
        @SuppressWarnings("unchecked")
        final List<String> multiuserClients = (List<String>)env.getProperty("multiuser.clients", List.class, Collections.emptyList());

        return new AccessControl(bannedUserList, multiuserClients, iqldb(), getDefaultLimits());
    }

    @Bean
    public RunningQueriesManager runningQueriesManager() {
        final RunningQueriesManager runningQueriesManager = new RunningQueriesManager(iqldb());
        runningQueriesManager.onStartup();
        return runningQueriesManager;
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
    public ImhotepClientPinger imhotepClientPinger() {
        return new ImhotepClientPinger(imhotepClient());
    }

    @Bean
    public ImhotepMetadataServiceClientPinger imhotepMetadataServiceClientPinger(ImsClientInterface imsClient) {
        return new ImhotepMetadataServiceClientPinger(imsClient, env.getProperty("ims.check_dataset"));
    }

    @Bean
    CORSInterceptor corsInterceptor() {
        return new CORSInterceptor(env);
    }

    @Bean
    WallClock clock() {
        return new DefaultWallClock();
    }

    // Configure default JSON serializer to produce pretty/indented output for human readability
    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
        builder.indentOutput(true);
        converters.add(new MappingJackson2HttpMessageConverter(builder.build()));
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(corsInterceptor());
    }

    @Override
    public void configureDefaultServletHandling(DefaultServletHandlerConfigurer configurer) {
        configurer.enable();
    }
}
