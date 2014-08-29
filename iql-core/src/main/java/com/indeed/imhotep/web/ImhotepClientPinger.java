package com.indeed.imhotep.web;

import com.indeed.imhotep.client.ImhotepClient;

/**
 * @author vladimir
 */

public class ImhotepClientPinger {
    private final ImhotepClient imhotepClient;

    public ImhotepClientPinger(ImhotepClient imhotepClient) {
        this.imhotepClient = imhotepClient;
    }

    public boolean isConnectionHealthy() {
        if(imhotepClient == null) {
            return false;
        }
        return imhotepClient.isConnectionHealthy();
    }
}
