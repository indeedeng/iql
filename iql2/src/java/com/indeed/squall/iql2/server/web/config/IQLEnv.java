package com.indeed.squall.iql2.server.web.config;

import org.springframework.core.env.Environment;

/**
 * @author vladimir
 */

public enum IQLEnv {
    PROD("prod"),
    STAGE("stage"),
    QA("qa"),
    INTEGRATION("integration"),
    DEVELOPER("developer");

    public final String id;

    IQLEnv(String id) {
        this.id = id;
    }

    public static boolean isSpringProfileSet(Environment environment) {
        for(IQLEnv env : IQLEnv.values()) {
            if(environment.acceptsProfiles(env.id)) {
                return true;
            }
        }

        return false;
    }

    public static IQLEnv fromSpring(Environment environment) {
        for(IQLEnv env : IQLEnv.values()) {
            if(environment.acceptsProfiles(env.id)) {
                return env;
            }
        }
        //TODO should this return null instead?
        return PROD;    // default
    }
}