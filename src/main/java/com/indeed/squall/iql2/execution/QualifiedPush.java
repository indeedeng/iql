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

package com.indeed.squall.iql2.execution;

import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author jwolfe
 */
public class QualifiedPush {
    private static final Logger log = Logger.getLogger(QualifiedPush.class);

    public final String sessionName;
    public final List<String> pushes;

    public QualifiedPush(String sessionName, List<String> pushes) {
        this.sessionName = sessionName;
        this.pushes = pushes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QualifiedPush that = (QualifiedPush) o;

        if (!pushes.equals(that.pushes)) return false;
        if (!sessionName.equals(that.sessionName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = sessionName.hashCode();
        result = 31 * result + pushes.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "QualifiedPush{" +
                "sessionName='" + sessionName + '\'' +
                ", pushes=" + pushes +
                '}';
    }
}
