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

package com.indeed.iql.web;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.springframework.core.env.PropertyResolver;
import org.springframework.jndi.JndiObjectFactoryBean;

import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 * @author vladimir
 */

public class DataSourceLoader {
    private static final Logger log = Logger.getLogger(DataSourceLoader.class);

    /**
     * Creates a data source using data (URL, username, password) from the properties or JNDI.
     * Throws an exception if the data source couldn't be constructed.
     * @param name name of the data source (resource name in jndi or prefix in properties)
     */
    public static DataSource getDataSource(String name, PropertyResolver properties) {
        String url = properties.getProperty(name + ".url");
        String username = properties.getProperty(name + ".username");
        String password = properties.getProperty(name + ".password");
        if(!Strings.isNullOrEmpty(username)) {
            BasicDataSource ds = new BasicDataSource();
            ds.setDriverClassName("com.mysql.jdbc.Driver");
            ds.setUrl(url);
            ds.setUsername(username);
            ds.setPassword(password);
            ds.setValidationQuery("SELECT 1");
            return ds;
        } else {  // fall back to JNDI
            JndiObjectFactoryBean jndi = new JndiObjectFactoryBean();
            jndi.setJndiName("java:comp/env/jdbc/" + name);
            jndi.setLookupOnStartup(true);
            jndi.setCache(true);
            jndi.setProxyInterface(javax.sql.DataSource.class);
            try {
                jndi.afterPropertiesSet();   // has to be called manually when using the factory
            } catch (NamingException e) {
                throw Throwables.propagate(e);
            }
            return (DataSource)jndi.getObject();
        }
    }

    /**
     * Creates a data source using data (URL, username, password) from the properties or JNDI.
     * Returns null if the data source couldn't be constructed instead of throwing an exception.
     * @param name name of the data source (resource name in jndi or prefix in properties)
     */
    public static DataSource tryGetDataSource(String name, PropertyResolver properties) {
        try {
            return getDataSource(name, properties);
        } catch (Exception ignored) {
            log.info("DataSource couldn't be loaded: " + name);
            return null;
        }
    }
}
