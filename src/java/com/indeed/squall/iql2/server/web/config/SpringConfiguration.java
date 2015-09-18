package com.indeed.squall.iql2.server.web.config;

import com.google.common.base.Strings;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.squall.iql2.server.LocalImhotepDaemon;
import com.indeed.squall.iql2.server.dimensions.DimensionsLoader;
import com.indeed.squall.iql2.server.web.AccessControl;
import com.indeed.squall.iql2.server.web.CORSInterceptor;
import com.indeed.squall.iql2.server.web.WebPackageMarker;
import com.indeed.squall.iql2.server.web.cache.QueryCache;
import com.indeed.squall.iql2.server.web.cache.QueryCacheFactory;
import com.indeed.squall.iql2.server.web.data.KeywordAnalyzerWhitelistLoader;
import com.indeed.squall.iql2.server.web.healthcheck.HealthcheckPackageMarker;
import com.indeed.squall.iql2.server.web.healthcheck.ImhotepClientPinger;
import com.indeed.squall.iql2.server.web.servlets.ServletsPackageMarker;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import javax.xml.bind.PropertyException;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableWebMvc
@EnableScheduling
@ComponentScan(basePackageClasses = {ConfigurationPackageMarker.class, ServletsPackageMarker.class, WebPackageMarker.class, HealthcheckPackageMarker.class})
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
    public DimensionsLoader dimensionsLoader(ScheduledThreadPoolExecutor executor) {
        final DimensionsLoader dimensionsLoader = new DimensionsLoader("dataset-dimensions", new File(env.getProperty("ramses.metadata.dir")));
        executor.scheduleAtFixedRate(dimensionsLoader, 0, 5, TimeUnit.MINUTES);
        return dimensionsLoader;
    }

    @Bean
    public KeywordAnalyzerWhitelistLoader keywordAnalyzerWhitelistLoader(ScheduledThreadPoolExecutor executor, ImhotepClient imhotepClient) {
        final KeywordAnalyzerWhitelistLoader keywordAnalyzerWhitelistLoader = new KeywordAnalyzerWhitelistLoader("keyword-analyzer-whitelist", new File(env.getProperty("ramses.metadata.dir")), imhotepClient);
        executor.scheduleAtFixedRate(keywordAnalyzerWhitelistLoader, 0, 5, TimeUnit.MINUTES);
        return keywordAnalyzerWhitelistLoader;
    }

    @Bean
    public AccessControl accessControl() {
        @SuppressWarnings("unchecked")
        final List<String> bannedUserList = (List<String>)env.getProperty("banned.users", List.class, Collections.emptyList());
        return new AccessControl(bannedUserList);
    }

    @Bean
    public ImhotepClientPinger imhotepClientPinger() {
        return new ImhotepClientPinger(imhotepClient());
    }

    @Bean
    CORSInterceptor corsInterceptor() {
        return new CORSInterceptor(env);
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(corsInterceptor());
    }
}
