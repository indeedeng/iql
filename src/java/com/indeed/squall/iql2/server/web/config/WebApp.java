package com.indeed.squall.iql2.server.web.config;

import com.indeed.squall.common.web.AbstractWebAppInitializer;

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
