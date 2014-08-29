package com.indeed.imhotep.web.config;

import com.google.common.base.Strings;
import com.indeed.imhotep.web.ImhotepClientPinger;
import com.indeed.util.core.threads.NamedThreadFactory;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.iql.cache.QueryCache;
import com.indeed.imhotep.iql.cache.QueryCacheFactory;
import com.indeed.imhotep.web.ImhotepMetadataCache;
import com.indeed.imhotep.web.QueryServlet;
import com.indeed.imhotep.web.TopTermsCache;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.servlet.config.annotation.DefaultServletHandlerConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.PropertyException;

@Configuration
@EnableWebMvc
@EnableScheduling
@ComponentScan(basePackageClasses = {SpringConfiguration.class,QueryServlet.class})
public class SpringConfiguration extends WebMvcConfigurerAdapter {
    @Autowired
    Environment env;

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

    private ImhotepClient getImhotepClient(String zkNodes, String zkPath, String host, boolean quiet) {
        if(!Strings.isNullOrEmpty(host)) {
            String mergePoint = host.split(",")[0];
            String[] mergePointParts = mergePoint.split(":");
            List<Host> hosts = Arrays.asList(new Host(mergePointParts[0], Integer.parseInt(mergePointParts[1])));
            return new ImhotepClient(hosts);
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
        return new ImhotepMetadataCache(imhotepClient(), env.getProperty("ramses.metadata.dir"), env.getProperty("disabled.fields"));
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
