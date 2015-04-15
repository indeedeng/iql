package com.indeed.imhotep.web;

import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;

/**
 * @author vladimir
 */

public class AccessControl {

    final Set<String> bannedUsers;

    public AccessControl(Collection<String> bannedUsers) {
        this.bannedUsers = Sets.newHashSet(bannedUsers);
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
