package com.indeed.imhotep.web;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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

    final Set<String> bannedUsers;
    final Set<String> multiuserClients;
    @Nullable private final IQLDB iqldb;
    private final Limits defaultLimits;
    private Map<String, Limits> identityToLimits = Maps.newHashMap();

    public AccessControl(Collection<String> bannedUsers, Collection<String> multiuserClients, @Nullable IQLDB iqldb, Limits defaultLimits) {
        this.bannedUsers = Sets.newHashSet(bannedUsers);
        this.multiuserClients = Sets.newHashSet(multiuserClients);
        this.iqldb = iqldb;
        this.defaultLimits = defaultLimits;
        updateLimits();
    }

    @Scheduled(fixedDelay = 5 * 60 * 1000)
    synchronized public void updateLimits() {
        try {
            if(iqldb != null) {
                identityToLimits = iqldb.getAccessLimits();
            }
        } catch (Exception e) {
            log.error("Error during update of limits", e);
        }
    }

    public Limits getLimitsForIdentity(String username, String client) {
        // We have 2 limits sub-hierarchies
        final boolean isMultiuserClient = isMultiuserClient(client);

        // First see if we have custom limits for this username and client
        final String identity = isMultiuserClient ? username : client;
        Limits limits = identityToLimits.get(identity);
        if(limits != null) {
            return limits;
        }

        // Try to use the default permissions from the DB
        final String defaultLimitsGroup = isMultiuserClient ? "defaultuser" : "defaultclient";
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


    public void checkAllowedAccess(String username) {
        if(bannedUsers.contains(username)) {
            throw new AccessDeniedException("Access denied");
        }
    }

    public static class AccessDeniedException extends RuntimeException {
        public AccessDeniedException(String message) {
            super(message);
        }
    }
}
