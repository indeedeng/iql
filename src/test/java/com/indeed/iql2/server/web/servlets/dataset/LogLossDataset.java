package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.ArrayList;
import java.util.List;

public class LogLossDataset {
    static Dataset create() {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        final FlamdexDocument doc = new FlamdexDocument();
        doc.setIntField("label", 1);
        doc.setIntField("score", 90);
        flamdex.addDocument(doc.copy());
        doc.setIntField("label", 1);
        doc.setIntField("score", 50);
        flamdex.addDocument(doc.copy());
        doc.setIntField("label", 1);
        doc.setIntField("score", 95);
        flamdex.addDocument(doc.copy());
        doc.setIntField("label", 1);
        doc.setIntField("score", 60);
        flamdex.addDocument(doc.copy());
        doc.setIntField("label", 1);
        doc.setIntField("score", 30);
        flamdex.addDocument(doc.copy());
        doc.setIntField("label", 0);
        doc.setIntField("score", 10);
        flamdex.addDocument(doc.copy());
        doc.setIntField("label", 0);
        doc.setIntField("score", 50);
        flamdex.addDocument(doc.copy());
        doc.setIntField("label", 0);
        doc.setIntField("score", 5);
        flamdex.addDocument(doc.copy());
        doc.setIntField("label", 0);
        doc.setIntField("score", 40);
        flamdex.addDocument(doc.copy());
        doc.setIntField("label", 0);
        doc.setIntField("score", 70);
        flamdex.addDocument(doc.copy());
        shards.add(new Dataset.DatasetShard("logloss", "index20150101", flamdex));
        return new Dataset(shards);
    }
}
