package com.indeed.squall.iql2.server.web.config;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePropertySource;
import org.springframework.web.context.ConfigurableWebApplicationContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @author vladimir
 */

/**
 * Adds Spring property sources for the following locations in the following order:
 * WEB-INF/config/iql2-server-base.properties
 * WEB-INF/config/iql2-server-${environment}.properties where environment is one of constants from ProfileIds for the active profile.
 * ${catalina.base)/conf/iql.properties
 * Path pointed to by propertyPlaceholderResourceLocation (or configFile) Tomcat context parameter
 *
 * This class can be extended to customize the properties file name and paths.
 */
@SuppressWarnings("UnusedDeclaration")
public class PropertiesInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    private static final Logger log = Logger.getLogger(PropertiesInitializer.class);
    private static final String contextPropertiesParameterName = "propertyPlaceholderResourceLocation";
    private static final String contextPropertiesParameterName2 = "configFile";
    private static final String indeedEnvironmentJVMProperty = "indeed.staging.level";


    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        final ConfigurableEnvironment springEnv = applicationContext.getEnvironment();

        activateSpringProfiles(springEnv);

        final MutablePropertySources propSources = springEnv.getPropertySources();

        for(String location : getPropertyLocations(applicationContext)) {
            tryAddPropertySource(applicationContext, propSources, location);
        }

        addPropertySources(applicationContext, propSources);
    }

    private static void activateSpringProfiles(ConfigurableEnvironment springEnv) {
        if(IQLEnv.isSpringProfileSet(springEnv)) {
            return; // we seem to already have some profile set. let it be
        }

        // try to infer the appropriate Spring profile
        // this JVM system property should be set in OPs managed JVMs
        String indeedEnv = System.getProperty(indeedEnvironmentJVMProperty);
        if(Strings.isNullOrEmpty(indeedEnv)) {
            indeedEnv = "developer"; // assume this is not an OPs managed JVM and thus a developer station
        }
        final List<String> profiles = Lists.newArrayList(springEnv.getActiveProfiles());
        profiles.add(indeedEnv);
        springEnv.setActiveProfiles(profiles.toArray(new String[profiles.size()]));
    }

    protected String getWebappName() {
        return "iql2-server";
    }

    /** Config directory path inside the project in SVN.
     * Should end with a /
     */
    protected String getSVNConfigLocation() {
        return "WEB-INF/config/";
    }

    protected List<String> getPropertyLocations(ConfigurableApplicationContext applicationContext) {
        List<String> propertyLocations = Lists.newArrayList();

        propertyLocations.addAll(getBasePropertyLocations(applicationContext));

        propertyLocations.addAll(getPerEnvironmentPropertyLocations(applicationContext));

        propertyLocations.addAll(getTomcatConfPropertyLocations(applicationContext));

        propertyLocations.addAll(getTomcatContextPropertyLocations(applicationContext));

        return propertyLocations;
    }

    protected List<String> getBasePropertyLocations(ConfigurableApplicationContext applicationContext) {
        String configFile = getSVNConfigLocation() + getConfigFileName("base");
        return Lists.newArrayList(configFile);
    }

    protected List<String> getPerEnvironmentPropertyLocations(ConfigurableApplicationContext applicationContext) {
        final ConfigurableEnvironment springEnv = applicationContext.getEnvironment();
        final IQLEnv env = IQLEnv.fromSpring(springEnv);
        List<String> locations = Lists.newArrayList();
        if(env != null) {
            locations.add(getSVNConfigLocation() + getConfigFileName(env.id));
            if(env.equals(IQLEnv.DEVELOPER)) {    // allow optional local override file (e.g. for private auth data)
                locations.add(getSVNConfigLocation() + getConfigFileName(env.id + "-private"));
            }
        }
        return locations;
    }

    protected List<String> getTomcatConfPropertyLocations(ConfigurableApplicationContext applicationContext) {
        String tomcatPropFile = getTomcatConfDir() + getConfigFileName(null);
        return Lists.newArrayList(tomcatPropFile);
    }

    protected String getTomcatConfDir() {
        return "file:" + System.getProperty("catalina.base") + "/conf/";
    }

    protected String getConfigFileName(String suffix) {
        String fileName = getWebappName();
        if(!Strings.isNullOrEmpty(suffix)) {
            fileName += "-" + suffix;
        }
        fileName += ".properties";
        return fileName;
    }

    protected List<String> getContextPropertiesParameterNames() {
        return Lists.newArrayList(contextPropertiesParameterName, contextPropertiesParameterName2);
    }

    protected List<String> getTomcatContextPropertyLocations(ConfigurableApplicationContext applicationContext) {
        if(!(applicationContext instanceof ConfigurableWebApplicationContext)) {
            return Collections.emptyList();
        }
        ConfigurableWebApplicationContext webApplicationContext = (ConfigurableWebApplicationContext) applicationContext;
        List<String> locations = Lists.newArrayList();
        for(String propertiesParameterName : getContextPropertiesParameterNames()) {
            final String tomcatContextPropertiesFile = webApplicationContext.getServletContext().getInitParameter(propertiesParameterName);
            locations.add("file:" + tomcatContextPropertiesFile);
        }
        return locations;
    }

    protected boolean tryAddPropertySource(ConfigurableApplicationContext applicationContext, MutablePropertySources propSources, String filePath) {
        if(filePath == null) {
            return false;
        }
        Resource propertiesResource = applicationContext.getResource(filePath);
        if(!propertiesResource.exists()) {
            return false;
        }
        try {
            ResourcePropertySource propertySource = new ResourcePropertySource(propertiesResource);
            propSources.addFirst(propertySource);
        } catch (IOException e) {
            return false;
        }
        log.debug("Successfully added property source: " + filePath);
        return true;
    }

    /**
     * Can be overridden to add custom property sources
     * @param applicationContext context to use for loading
     * @param propSources where to append to
     */
    protected void addPropertySources(ConfigurableApplicationContext applicationContext, MutablePropertySources propSources) { }

}