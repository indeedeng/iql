/*
 * Copyright (C) 2017 Indeed Inc.
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
package com.indeed.imhotep.shortlink;

import org.apache.log4j.Logger;
import org.springframework.core.env.PropertyResolver;

import javax.xml.bind.PropertyException;
import java.io.IOException;

public class ShortLinkRepositoryFactory {
    static final Logger log = Logger.getLogger(ShortLinkRepositoryFactory.class);
    
    public static final ShortLinkRepository newShortLinkRepository(PropertyResolver props) throws PropertyException {
        final String repoType;
        boolean enabled;
        
        enabled = props.getProperty("shortlink.enabled", Boolean.class, true);
        if(!enabled) {
            log.info("Shortlinking disabled in config");
            return new NoOpRepository();
        }
        repoType = props.getProperty("shortlink.backend", String.class, "S3");
        if ("S3".equals(repoType)) {
            return new S3ShortLinkRepository(props);
        }
        
        throw new PropertyException("Unknown cache type (property: query.cache.backend): "
                + repoType);
    }
    
    static class NoOpRepository implements ShortLinkRepository {

        @Override
        public boolean mapShortCode(String code, String query, String view) throws IOException {
            return false;
        }

        @Override
        public String resolveShortCode(String shortCode) throws IOException {
            return null;
        }

        @Override
        public boolean isEnabled() {
            return false;
        }
    }

}
