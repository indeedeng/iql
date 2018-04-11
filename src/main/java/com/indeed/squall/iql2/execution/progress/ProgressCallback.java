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

package com.indeed.squall.iql2.execution.progress;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.commands.Command;

import java.util.Map;

public interface ProgressCallback {
    /**
     * Optional.absent() when a Command List is not given.
     */
    void startSession(Optional<Integer> numCommands);

    // Provided separately in order to allow early termination.
    void sessionOpened(ImhotepSession session);

    void sessionsOpened(Map<String, Session.ImhotepSessionInfo> sessions);

    void startCommand(Session session, Command command, boolean streamingToTSV);

    void endCommand(Session session, Command command);
}
