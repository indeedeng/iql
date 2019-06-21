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

package com.indeed.iql2.server.web.servlets;

import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.iql.Constants;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.varexport.VarExporter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.IOException;

/**
 * @author zheli
 */
@Ignore
public abstract class BasicTest {
    public static final DateTimeZone TIME_ZONE = Constants.DEFAULT_IQL_TIME_ZONE;
    private static Level level;

    @BeforeClass public static void disbaleVarExporterWarning() {
        level = Logger.getLogger(VarExporter.class).getLevel();
        Logger.getLogger(VarExporter.class).setLevel(Level.OFF);
        try (ImhotepClient normalClient = AllData.DATASET.getNormalClient()) {
        } catch (IOException ignored) {
        }
        try (ImhotepClient dimensionsClient = AllData.DATASET.getDimensionsClient()) {
        } catch (IOException ignored) {
        }
    }

    @AfterClass public static void enableVarExporter() {
        Logger.getLogger(VarExporter.class).setLevel(level);
    }
}
