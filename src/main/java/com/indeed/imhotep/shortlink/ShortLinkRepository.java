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

import java.io.IOException;

/**
 * Abstracts the storage for IQL query shortlink codes
 */
public interface ShortLinkRepository {

    /**
     * Maps a short code to an IQL query string. The repository is write-once, so this method
     * will modify nothing and return false if the short code is already in use.
     *
     * @param code short code to map
     * @param query IQL query for short code
     * @throws IOException if unable to write mapping to repository
     * @return true if mapping succeeded, false if the short code is already in use.
     */
    boolean mapShortCode(String code, String query) throws IOException;

    /**
     * Returns the IQL query string for a given short code
     *
     * @param shortCode short code to look up
     * @return IQL query string (*not URL-encoded) or null if none found
     */
    String resolveShortCode(String shortCode) throws IOException;

    /**
     * @return true if short linking is enabled
     */
    boolean isEnabled();
}
