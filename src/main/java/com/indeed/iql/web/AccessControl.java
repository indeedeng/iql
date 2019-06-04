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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.iql.exceptions.IqlKnownException;
import org.apache.log4j.Logger;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author vladimir
 */

public class AccessControl {
    private static final Logger log = Logger.getLogger(AccessControl.class);
    public static final String MULTIUSER_CLIENT_DEFAULT_IDENTITY = "defaultuser";
    public static final String SINGLEUSER_CLIENT_DEFAULT_IDENTITY = "defaultclient";

    private final Set<String> bannedUsers;
    private final Set<String> multiuserClients;
    private final Set<String> interactiveClients;
    @Nullable private final IQLDB iqldb;
    private final Limits defaultLimits;
    private final Set<String> privilegedDatasets;
    private final Set<String> privilegedDatasetsUsers;
    private Map<String, Limits> identityToLimits = Maps.newHashMap();

    public AccessControl(Collection<String> bannedUsers, Collection<String> multiuserClients,
                         Collection<String> interactiveClients, @Nullable IQLDB iqldb, Limits defaultLimits,
                         Set<String> privilegedDatasets, Set<String> privilegedDatasetsUsers) {
        this.bannedUsers = Sets.newHashSet(bannedUsers);
        this.multiuserClients = Sets.newHashSet(multiuserClients);
        this.interactiveClients = Sets.newHashSet(interactiveClients);
        this.iqldb = iqldb;
        this.defaultLimits = defaultLimits;
        this.privilegedDatasets = Sets.newHashSet(privilegedDatasets);
        this.privilegedDatasetsUsers = Sets.newHashSet(privilegedDatasetsUsers);
        updateLimits();
    }

    @Scheduled(fixedDelay = 5 * 60 * 1000)
    public synchronized void updateLimits() {
        try {
            if(iqldb != null) {
                identityToLimits = iqldb.getAccessLimits();
            }
        } catch (Exception e) {
            log.error("Error during update of limits", e);
        }
    }

    public Limits getLimitsForIdentity(String username, String client) {
        final Limits dbBasedLimits = getDBLimitsForIdentity(username, client);
        // TODO: move priority to the DB tbllimits
        final boolean isInteractiveClient = interactiveClients.contains(client);
        final byte priority = (byte) (isInteractiveClient ? 50 : 0);
        return new Limits(priority, dbBasedLimits.queryDocumentCountLimitBillions, dbBasedLimits.queryInMemoryRowsLimit,
                dbBasedLimits.queryFTGSIQLLimitMB, dbBasedLimits.queryFTGSImhotepDaemonLimitMB,
                dbBasedLimits.concurrentQueriesLimit, dbBasedLimits.concurrentImhotepSessionsLimit);
    }


    private Limits getDBLimitsForIdentity(String username, String client) {
        // We have 2 limits sub-hierarchies
        final boolean isMultiuserClient = isMultiuserClient(client);

        // First see if we have custom limits for this username and client
        final String identity = isMultiuserClient ? username : client;
        Limits limits = identityToLimits.get(identity);
        if(limits != null) {
            return limits;
        }

        // Try to use the default permissions from the DB
        final String defaultLimitsGroup = isMultiuserClient ? MULTIUSER_CLIENT_DEFAULT_IDENTITY : SINGLEUSER_CLIENT_DEFAULT_IDENTITY;
        limits = identityToLimits.get(defaultLimitsGroup);
        if(limits != null) {
            return limits;
        }

        // Fallback to the default set in the app's configs
        return defaultLimits;
    }

    public boolean isMultiuserClient(String client) {
        return client.isEmpty() || multiuserClients.contains(client);
    }


    public void checkAllowedIQLAccess(String username, String client) {
        if(bannedUsers.contains(username) ||
                !getLimitsForIdentity(username, client).satisfiesConcurrentQueriesLimit(1)) {
            throw new IqlKnownException.AccessDeniedException("Access denied");
        }
    }

    public boolean userCanAccessDataset(String username, String datasetName) {
        return !privilegedDatasets.contains(datasetName) || privilegedDatasetsUsers.contains(username);
    }

    public void checkAllowedDatasetAccess(String username, String datasetName) {
        if (!userCanAccessDataset(username, datasetName)) {
            throw new IqlKnownException.AccessDeniedException("Access denied");
        }
    }
}
