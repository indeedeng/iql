package com.indeed.squall.iql2.server;

import com.indeed.imhotep.client.ImhotepClient;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Configuration
@EnableWebMvc
@EnableScheduling
@ComponentScan(basePackageClasses = {SpringConfiguration.class, Server.class})
public class SpringConfiguration extends WebMvcConfigurerAdapter {
    private static final Logger log = Logger.getLogger(SpringConfiguration.class);

    // TODO: Configure this the same way opensource/iql does
    @Bean(destroyMethod = "close")
    public ImhotepClient imhotepClient() throws IOException {
        final Properties props = new Properties();
        final InputStream propsStream = SpringConfiguration.class.getResourceAsStream("config.properties");
        if (propsStream != null) {
            props.load(propsStream);
        }
        String zkPath = (String) props.get("zk_path");
        if (zkPath == null) {
            zkPath = "***REMOVED***";
        }
        log.info("zkPath = " + zkPath);

        return new ImhotepClient(zkPath, true);
    }
}
