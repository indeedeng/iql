package com.indeed.imhotep.web;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ClientInfo {
    public final String username;
    public final String client;
    @JsonIgnore
    public final boolean isMultiuserClient;
    // TODO: add more fields
//    public final String author;
//    public final String clientExecutionId;
//    public final String clientId;


    public ClientInfo(String username, String client, boolean isMultiuserClient) {
        this.username = username;
        this.client = client;
        this.isMultiuserClient = isMultiuserClient;
    }
}
