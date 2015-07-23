package com.indeed.squall.iql2.server.web;

import com.google.common.collect.ImmutableSet;

import java.util.Collection;

/**
 * @author vladimir
 */

public class AccessControl {

    final ImmutableSet<String> bannedUsers;

    public AccessControl(Collection<String> bannedUsers) {
        this.bannedUsers = ImmutableSet.copyOf(bannedUsers);
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
