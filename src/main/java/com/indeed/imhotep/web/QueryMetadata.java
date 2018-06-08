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
 package com.indeed.imhotep.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author vladimir
 */

public class QueryMetadata {
    private final List<QueryMetadataItem> items = Lists.newArrayList();

    public void addItem(String name, Object value) {
        addItem(name, value, true);
    }

    public void addItem(String name, Object value, boolean sendPending) {
        items.add(new QueryMetadataItem(name, value, sendPending));
    }

    public String toJSON() {
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode headerObject = mapper.createObjectNode();
        for(QueryMetadataItem queryMetadataItem : items) {
            headerObject.put(queryMetadataItem.name, queryMetadataItem.value);
        }
        return headerObject.toString();
    }

    public void setPendingHeaders(HttpServletResponse resp) {
        for(QueryMetadataItem queryMetadataItem : items) {
            if(queryMetadataItem.sendPending) {
                resp.setHeader(queryMetadataItem.name, queryMetadataItem.value);
                queryMetadataItem.markSent();
            }
        }
    }

    /**
     * Serializes this object to the stream as JSON.
     * Closes the stream after.
     */
    public void toStream(OutputStream outputStream) {
        final String stringSerialization = toJSON();
        try {
            final OutputStreamWriter outputStreamWriter = new OutputStreamWriter(new BufferedOutputStream(outputStream), Charsets.UTF_8);
            outputStreamWriter.write(stringSerialization);
            outputStreamWriter.close();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }


    public static QueryMetadata fromStream(InputStream inputStream) {
        final String stringVal = streamToString(inputStream);
        return fromJSON(stringVal);
    }

    public static QueryMetadata fromJSON(String json) {
        final QueryMetadata metadataObject = new QueryMetadata();
        final ObjectMapper mapper = new ObjectMapper();
        try {
            final JsonNode root = mapper.readTree(json);
            final Iterator<Map.Entry<String, JsonNode>> nodeIterator = root.fields();
            while(nodeIterator.hasNext()) {
                Map.Entry<String, JsonNode> item = nodeIterator.next();
                metadataObject.addItem(item.getKey(), item.getValue().textValue());
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return metadataObject;
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
                addItem(otherItem.name, otherItem.value);
            }
        }
    }

    public QueryMetadata copy() {
        final QueryMetadata newInstance = new QueryMetadata();
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
        public final String name;
        public final String value;
        public boolean sendPending;

        public QueryMetadataItem(String name, Object value, boolean sendPending) {
            this.name = name;
            this.value = value == null ? "" : String.valueOf(value);
            this.sendPending = sendPending;
        }

        public void markSent() {
            sendPending = false;
        }
    }
}
