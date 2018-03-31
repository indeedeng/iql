package com.indeed.squall.iql2.execution;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.commands.Command;
import com.indeed.squall.iql2.execution.commands.TimePeriodRegroup;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestIt {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    @Test
    public void letsDoIt() throws IOException, ImhotepOutOfMemoryException {
        final List<Document> documents = new ArrayList<>();
        documents.add(
            Document.builder("organic", new DateTime(2015, 1, 1, 0, 0).getMillis())
                    .build()
        );
        final List<Command> commands = new ArrayList<>();
        TestUtil.testOne(documents, commands, new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0));
    }

    @Test
    public void testGroupByTime() throws IOException, ImhotepOutOfMemoryException {
        final List<Document> documents = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            documents.add(Document.builder("organic", new DateTime(2015, 1, 1, i, 0).getMillis()).build());
        }
        final List<Command> commands = new ArrayList<>();
        commands.add(new TimePeriodRegroup(1000 * 60 * 60 /* 1 h */, Optional.<String>absent(), Optional.<String>absent(), false));
        TestUtil.testOne(documents, commands, new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 2, 0, 0));
    }

}
