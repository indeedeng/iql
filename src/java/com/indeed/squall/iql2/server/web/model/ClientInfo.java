package com.indeed.squall.iql2.server.web.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ClientInfo {
    /** name of the user performing the query */
    public final String username;
    /** name of the user who composed the query */
    @JsonIgnore
    public final String author;
    /** name of the application performing the query */
    public final String client;
    /** unique id of the internal process happening in the client */
    @JsonIgnore
    public final String clientProcessId;
    /** string name of the internal process happening in the client */
    public final String clientProcessName;
    /** unique id of the internal process execution in the client */
    @JsonIgnore
    public final String clientExecutionId;

    /** Whether this client allows multiple users to run queries */
    @JsonIgnore
    public final boolean isMultiuserClient;

    public ClientInfo(String username, String author, String client, String clientProcessId, String clientProcessName, String clientExecutionId, boolean isMultiuserClient) {
        this.username = username;
        this.author = author;
        this.client = client;
        this.clientProcessId = clientProcessId;
        this.clientProcessName = clientProcessName;
        this.clientExecutionId = clientExecutionId;
        this.isMultiuserClient = isMultiuserClient;
    }
}

