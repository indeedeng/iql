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
import com.google.common.collect.Maps;
import com.indeed.util.varexport.Export;
import com.indeed.util.varexport.VarExporter;
import com.indeed.util.varexport.servlet.ViewExportedVariablesServlet;
import com.indeed.imhotep.iql.IQLQuery;
import com.indeed.imhotep.web.NoCacheFilter;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


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
            if(tempFile.isFile() && tempFile.getName().startsWith(IQLQuery.TEMP_FILE_PREFIX)) {
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

    protected void initVarExport(ServletContext servletContext) {
        SystemExports.register(true);

        final ServletRegistration.Dynamic statusServlet = servletContext.addServlet("ViewExportedVariablesServlet", ViewExportedVariablesServlet.class);
        statusServlet.setLoadOnStartup(1);
        statusServlet.addMapping("/v");
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

    public static class SystemExports {
	private static AtomicBoolean registered = new AtomicBoolean(false);

        private final Map<String, String> systemProperties = Maps.newHashMap();

	public static final void register(final boolean includeInGlobal) {
	    if (registered.compareAndSet(false, true)) {
                new SystemExports(includeInGlobal);
            }
        }

        SystemExports(final boolean includeInGlobal) {
            for (final Map.Entry<Object, Object> prop : System.getProperties().entrySet()) {
                systemProperties.put((String) prop.getKey(), (String) prop.getValue());
            }
            final VarExporter e = VarExporter.forNamespace("system");
            if (includeInGlobal) {
                e.includeInGlobal();
            }
            e.export(this, "");
        }

        @Export(name="env", expand=true)
        public Map<String, String> getEnv() {
            return System.getenv();
        }

        @Export(name="properties", expand=true)
        public Map<String, String> getProperties() {
            return systemProperties;
        }
    }
}

