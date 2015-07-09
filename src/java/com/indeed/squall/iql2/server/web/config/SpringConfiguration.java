package com.indeed.squall.iql2.server.web.config;

import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.squall.iql2.server.web.CORSInterceptor;
import com.indeed.squall.iql2.server.web.cache.QueryCache;
import com.indeed.squall.iql2.server.web.cache.QueryCacheFactory;
import com.indeed.squall.iql2.server.web.servlets.ServletsPackageMarker;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import javax.xml.bind.PropertyException;
import java.io.IOException;

@Configuration
@EnableWebMvc
@EnableScheduling
@ComponentScan(basePackageClasses = {ConfigurationPackageMarker.class, ServletsPackageMarker.class})
public class SpringConfiguration extends WebMvcConfigurerAdapter {
    private static final Logger log = Logger.getLogger(SpringConfiguration.class);

    @Autowired
    Environment env;

    @Bean(destroyMethod = "close")
    public ImhotepClient imhotepClient() throws IOException {
        String zkPath = env.getProperty("imhotep.daemons.zookeeper.path", "");
        String zkNodes = env.getProperty("imhotep.daemons.zookeeper.quorum", "***REMOVED***");
        if (zkPath.isEmpty()) {
            return new ImhotepClient(zkNodes, true);
        } else {
            return new ImhotepClient(zkNodes, zkPath, true);
        }
    }

    @Bean
    QueryCache queryCache() throws PropertyException {
        return QueryCacheFactory.newQueryCache(env);
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
