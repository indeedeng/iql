package com.indeed.imhotep.web;

import com.google.common.base.Joiner;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class QueryLogEntry implements Iterable<Entry<String,String>> {
    private Map<String, String> propertyMap;

    public QueryLogEntry() {
        this.propertyMap = new LinkedHashMap<String, String>();
    }

    public void setProperty(String key, String val) {
        propertyMap.put(key, val);
    }

    public void setProperty(String key, long val) {
        propertyMap.put(key, Long.toString(val));
    }

    public void setProperty(String key, int val) {
        propertyMap.put(key, Integer.toString(val));
    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        return propertyMap.entrySet().iterator();
    }

    @Override
    public String toString() {
        return Joiner.on(" ").withKeyValueSeparator(":").join(propertyMap);
    }
}
