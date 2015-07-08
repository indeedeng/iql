package com.indeed.squall.iql2.server.web.config;

import com.indeed.squall.common.web.AbstractWebAppInitializer;
import com.indeed.squall.iql2.server.web.config.SpringConfiguration;

import javax.servlet.ServletRegistration;

public class WebApp extends AbstractWebAppInitializer {
    @Override
    protected Class<?>[] getServletConfigClasses() {
        return new Class<?>[]{SpringConfiguration.class};
    }

    @Override
    protected String getPropertiesInitializerClass() {
        return PropertiesInitializer.class.getName();
    }
}
