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

import com.google.common.base.Strings;
import com.indeed.iql.web.filters.LargePostFilter;
import com.indeed.iql1.iql.IQLQuery;
import com.indeed.iql.web.filters.NoCacheFilter;
import org.apache.log4j.Logger;
import org.springframework.web.WebApplicationInitializer;
import org.springframework.web.filter.CharacterEncodingFilter;
import org.springframework.web.servlet.support.AbstractAnnotationConfigDispatcherServletInitializer;
import org.springframework.web.util.Log4jConfigListener;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;
import java.io.File;
import java.util.EnumSet;


/**
 * @author vladimir
 */

/**
 * Initializes a generic internal Spring config based webapp.
 * Things initialized: log4j, DBStatusManager, VarExport, JSP, Healthchecks.
 */
public class WebApp  extends AbstractAnnotationConfigDispatcherServletInitializer
                                    implements WebApplicationInitializer {
    private static final Logger log = Logger.getLogger(WebApp.class);
    private static boolean started = false;

    @Override
    public void onStartup(ServletContext servletContext) throws ServletException {
        if(WebApp.started) {
            return; // avoid double initialization when a subclass exists
        }
        WebApp.started = true;

        initWebapp(servletContext);

        initCharacterEncodingFilter(servletContext);

        super.onStartup(servletContext);

        FilterRegistration.Dynamic noCacheFilter = servletContext.addFilter("nocache", NoCacheFilter.class);
        noCacheFilter.addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, "/*");

        servletContext.addFilter("largepost", LargePostFilter.class)
            .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, "/*");

        cleanupTempFiles();
    }

    @Override
    protected Class<?>[] getServletConfigClasses() {
        return new Class<?>[] {SpringConfiguration.class};
    }

    private static void cleanupTempFiles() {
        final String tempDirPath = System.getProperty("java.io.tmpdir");
        final File tempDir = new File(tempDirPath);
        final File[] files = tempDir.listFiles();
        if(files == null) {
            return;
        }
        int deletedCount = 0;
        for(final File tempFile : files) {
            final String fileName = tempFile.getName();
            if(tempFile.isFile() && (
                    fileName.startsWith(IQLQuery.TEMP_FILE_PREFIX) || // IQL temp file
                    fileName.startsWith("query") && fileName.endsWith(".cache.tmp") || // IQL2 temp file
                    fileName.startsWith("ftgs") && fileName.endsWith(".tmp") ||  // Imhotep FTGS temp file
                    fileName.startsWith("batchGroupStatsIterator") && fileName.endsWith(".tmp") || // Batch Request temp file
                    fileName.startsWith("groupStatsIterator") && fileName.endsWith(".tmp") // GetGroupStats temp file
            )) {
                if(!tempFile.delete()) {
                    log.warn("Failed to delete temp file: " + tempFile);
                }
                deletedCount++;
            }
        }
        if(deletedCount > 0) {
            log.info("Cleaned up " + deletedCount + " temp files");
        }
    }

    protected void initWebapp(ServletContext servletContext) {
        initLog4j(servletContext);

        initJSPMapping(servletContext);
    }

    protected void initLog4j(ServletContext servletContext) {
        final String log4jConfigLocationParam = "log4jConfigLocation";
        final String log4jConfigLocation = servletContext.getInitParameter(log4jConfigLocationParam);
        if(Strings.isNullOrEmpty(log4jConfigLocation)) {
            servletContext.setInitParameter(log4jConfigLocationParam, getDefaultLog4jConfigLocation());
        }
        servletContext.setInitParameter("log4jExposeWebAppRoot", "false");

        servletContext.addListener(Log4jConfigListener.class);
    }

    protected void initJSPMapping(ServletContext servletContext) {
        // Map jsp files to jsp servlet
        servletContext.getServletRegistration("jsp").addMapping("*.jsp");
    }

    protected String getDefaultLog4jConfigLocation() {
        return "classpath:config/log4j.xml";
    }

    protected String getPropertiesInitializerClass() {
        return PropertiesInitializer.class.getName();
    }

    // Spring AbstractAnnotationConfigDispatcherServletInitializer overrides below

    @Override
    protected void customizeRegistration(ServletRegistration.Dynamic registration) {
        String initClass = getPropertiesInitializerClass();
        if(initClass != null) {
            registration.setInitParameter("contextInitializerClasses", initClass);
        }
        super.customizeRegistration(registration);
    }
    
    
    @Override
    protected String[] getServletMappings() {
        return new String[] { "/" };
    }

    @Override
    protected Class<?>[] getRootConfigClasses() {
        return new Class<?>[0]; // root ApplicationContext not used by default. everything is in the servlet context
    }

    private void initCharacterEncodingFilter(final ServletContext servletContext) {
        final CharacterEncodingFilter utf8 = new CharacterEncodingFilter();
        utf8.setEncoding("utf-8");
        utf8.setForceEncoding(true);
        servletContext.addFilter("SetCharacterEncodingFilter", utf8)
                .addMappingForUrlPatterns(null, false, "/*");
    }
}

