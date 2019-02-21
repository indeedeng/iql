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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.indeed.iql.cache.CompletableOutputStream;
import org.apache.commons.lang.StringUtils;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author vladimir
 */

public class QueryMetadata {
    private final List<QueryMetadataItem> items;

    final HttpServletResponse resp;

    QueryMetadata() {
        this(null);
    }

    public QueryMetadata(HttpServletResponse resp) {
        this.resp = resp;
        this.items = Lists.newArrayList();
    }

    public QueryMetadata(HttpServletResponse resp, List<QueryMetadataItem> items) {
        this.resp = resp;
        this.items = items;
    }

    public void addItem(String name, Object value, boolean sendAsHeader) {
        items.add(new QueryMetadataItem(name, value, sendAsHeader));
    }

    public void renameItem(String oldName, String newName) {
        for(QueryMetadataItem queryMetadataItem : items) {
            if(queryMetadataItem.name.equals(oldName)) {
                queryMetadataItem.name = newName;
            }
        }
    }

    public String toJSONForClients() {
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode headerObject = mapper.createObjectNode();
        for(QueryMetadataItem queryMetadataItem : items) {
            headerObject.put(queryMetadataItem.name, queryMetadataItem.value);
        }
        return headerObject.toString();
    }

    public String toJSONForCaching() {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(items);
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    public void setPendingHeaders() {
        for(QueryMetadataItem queryMetadataItem : items) {
            if(queryMetadataItem.sendAsHeaderPending) {
                resp.setHeader(queryMetadataItem.name, queryMetadataItem.value);
                queryMetadataItem.markSent();
            }
        }
        resp.setHeader("Access-Control-Expose-Headers", StringUtils.join(resp.getHeaderNames(), ", "));
    }

    /**
     * Serializes this object to the stream as JSON.
     * Closes the stream after but only on success.
     */
    public void toOutputStream(CompletableOutputStream outputStream) {
        final String stringSerialization = toJSONForCaching();
        try (final OutputStreamWriter outputStreamWriter = new OutputStreamWriter(new BufferedOutputStream(outputStream), Charsets.UTF_8)) {
            outputStreamWriter.write(stringSerialization);
            // Don't consider successful if final flush before close() fails.
            outputStreamWriter.flush();
            outputStream.complete();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }


    public static QueryMetadata fromStream(InputStream inputStream) {
        final String stringVal = streamToString(inputStream);
        return fromJSON(stringVal, null);
    }

    public static QueryMetadata fromJSON(String json, HttpServletResponse resp) {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            List<QueryMetadataItem> items = mapper.readValue(json, new TypeReference<List<QueryMetadataItem>>() {});
            return new QueryMetadata(resp, items);
        } catch (Exception e) {
            // TODO: delete. fallback to try to read old format
            try {
                final QueryMetadata metadataObject = new QueryMetadata(resp);
                final JsonNode root = mapper.readTree(json);
                final Iterator<Map.Entry<String, JsonNode>> nodeIterator = root.fields();
                while (nodeIterator.hasNext()) {
                    Map.Entry<String, JsonNode> item = nodeIterator.next();
                    metadataObject.addItem(item.getKey(), item.getValue().textValue(), false);
                }
                return metadataObject;
            } catch (IOException ignored) {
                // fallback failed, propagate initial error
                throw Throwables.propagate(e);
            }
        }
    }

    /**
     * Adds metadata items from another QueryMetadata instance to this instance.
     */
    public void mergeIn(QueryMetadata otherInstance) {
        for(QueryMetadataItem otherItem : otherInstance.items) {
            boolean alreadyExists = false;
            for(QueryMetadataItem item : items) {
                if(item.name.equals(otherItem.name)) {
                    alreadyExists = true;
                }
            }
            if(!alreadyExists) {
                addItem(otherItem.name, otherItem.value, otherItem.sendAsHeader);
            }
        }
    }

    public QueryMetadata copy() {
        final QueryMetadata newInstance = new QueryMetadata(resp);
        newInstance.items.addAll(this.items);
        return newInstance;
    }

    private static String streamToString(InputStream inputStream) {
        try {
            final InputStreamReader inputStreamReader = new InputStreamReader(inputStream, Charsets.UTF_8);
            final String stringValue = CharStreams.toString(inputStreamReader);
            inputStreamReader.close();
            return stringValue;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static class QueryMetadataItem {
        public String name;
        public String value;
        public boolean sendAsHeader;
        @JsonIgnore
        public boolean sendAsHeaderPending;

        QueryMetadataItem() {
        }

        QueryMetadataItem(String name, Object value, boolean sendAsHeader) {
            this.name = name;
            this.value = value == null ? "" : String.valueOf(value);
            this.sendAsHeader = sendAsHeader;
            this.sendAsHeaderPending = sendAsHeader;
        }

        public void markSent() {
            sendAsHeaderPending = false;
        }
    }
}
