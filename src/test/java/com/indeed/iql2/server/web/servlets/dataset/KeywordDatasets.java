package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.ArrayList;
import java.util.List;

public class KeywordDatasets {
    static Dataset create() {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();

        final Dataset.DatasetFlamdex emptyFlamdex = new Dataset.DatasetFlamdex();
        shards.add(new Dataset.DatasetShard("from", "index20150101", emptyFlamdex));
        shards.add(new Dataset.DatasetShard("where", "index20150101", emptyFlamdex));
        shards.add(new Dataset.DatasetShard("group", "index20150101", emptyFlamdex));
        shards.add(new Dataset.DatasetShard("select", "index20150101", emptyFlamdex));
        shards.add(new Dataset.DatasetShard("limit", "index20150101", emptyFlamdex));

        final Dataset.DatasetFlamdex keywordsFlamdex = new Dataset.DatasetFlamdex();
        final FlamdexDocument.Builder doc = new FlamdexDocument.Builder();
        doc.addStringTerm("from", "from");
        doc.addStringTerm("where", "where");
        doc.addStringTerm("group", "group");
        doc.addStringTerm("by", "by");
        doc.addStringTerm("select", "select");
        doc.addStringTerm("limit", "limit");
        keywordsFlamdex.addDocument(doc.build());
        shards.add(new Dataset.DatasetShard("keywords", "index20150101", keywordsFlamdex));

        return new Dataset(shards);
    }
}
