package com.indeed.imhotep.web;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * @author vladimir
 */

public class DatasetStats {
    public String name;
    public int numShards;
    public int numStrFields;
    public int numIntFields;
    @JsonIgnore
    public Set<String> typeConflictFields = Sets.newHashSet();
    public int numTypeConflictFields;
    public long numDocs;
    public long lastShardNumDocs;
    public long lastWeekNumDocs;
    public List<Integer> shardSizesHours = Lists.newArrayList();
    @Nullable
    public DateTime firstShardBuildTimestamp;
    @Nullable
    public DateTime lastShardBuildTimestamp;

    public DateTime firstDataTime;
    public DateTime lastDataTime;

    public DateTime reportGenerationTime;

    public DatasetStats() {
    }

    public int getNumFields() {
        return numIntFields + numStrFields;
    }

    public String getShardType() {
        return numIntFields > 0 ? "flamdex" : "lucene";
    }

    public String getName() {
        return name;
    }

    public int getNumShards() {
        return numShards;
    }

    public int getNumStrFields() {
        return numStrFields;
    }

    public int getNumIntFields() {
        return numIntFields;
    }

    public int getNumTypeConflictFields() {
        return numTypeConflictFields;
    }

    public long getNumDocs() {
        return numDocs;
    }

    public List<Integer> getShardSizesHours() {
        return shardSizesHours;
    }

    public long getLastWeekNumDocs() {
        return lastWeekNumDocs;
    }

    public long getLastShardNumDocs() {
        return lastShardNumDocs;
    }

    private static final DateTimeFormatter yyyymmddhh = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH").withZone(DateTimeZone.forOffsetHours(-6));
    private static final DateTimeFormatter yyyymmdd = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.forOffsetHours(-6));

    @Nonnull
    private static String shardDateToString(@Nullable DateTime date) {
        if(date == null) {
            return "";
        }
        if (date.getMillisOfDay() == 0) {
            return date.toString(yyyymmdd);
        } else {
            return date.toString(yyyymmddhh);
        }
    }

    @Nonnull
    private static String shardBuildTimestampToString(@Nullable DateTime date) {
        if(date == null) {
            return "";
        }
        return date.toString().substring(0, 19);
    }

    public String getLastShardBuildTimestamp() {
        return shardBuildTimestampToString(lastShardBuildTimestamp);
    }

    public String getFirstShardBuildTimestamp() {
        return shardBuildTimestampToString(firstShardBuildTimestamp);
    }

    public String getFirstDataTime() {
        return shardDateToString(firstDataTime);
    }

    public String getLastDataTime() {
        return shardDateToString(lastDataTime);
    }

    public String getReportGenerationTime() {
        return shardDateToString(reportGenerationTime);
    }
}
