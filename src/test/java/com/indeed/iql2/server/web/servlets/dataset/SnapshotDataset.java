package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.iql.Constants;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;

/**
 * This dataset mimics datasets that are daily snapshots where a given entity will only occur
 * at most once on a given day, typically reflecting whether said entity is still live at that point.
 *
 * @author jwolfe
 */
public class SnapshotDataset {
    static Dataset create() {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        final List<List<Integer>> idPresences = ImmutableList.of(
                // Day presences --                    // DISTINCT_WINDOW(2)    -- DISTINCT_WINDOW(2, having count() > 1)
                ImmutableList.of(1, 1, 1, 0, 1, 0, 0), // [1, 1, 1, 1, 1, 1, 0] -- [0, 1, 1, 0, 0, 0, 0]
                ImmutableList.of(1, 1, 1, 1, 1, 1, 1), // [1, 1, 1, 1, 1, 1, 1] -- [0, 1, 1, 1, 1, 1, 1]
                ImmutableList.of(0, 1, 1, 1, 1, 1, 1), // [0, 1, 1, 1, 1, 1, 1] -- [0, 0, 1, 1, 1, 1, 1]
                ImmutableList.of(0, 1, 0, 0, 0, 0, 0), // [0, 1, 1, 0, 0, 0, 0] -- [0, 0, 0, 0, 0, 0, 0]
                ImmutableList.of(1, 0, 0, 0, 0, 0, 1), // [1, 1, 0, 0, 0, 0, 1] -- [0, 0, 0, 0, 0, 0, 0]
                ImmutableList.of(0, 0, 0, 0, 0, 0, 0), // [0, 0, 0, 0, 0, 0, 0] -- [0, 0, 0, 0, 0, 0, 0]
                ImmutableList.of(1, 0, 0, 0, 0, 0, 0), // [1, 1, 0, 0, 0, 0, 0] -- [0, 0, 0, 0, 0, 0, 0]
                ImmutableList.of(0, 0, 0, 0, 0, 0, 1), // [0, 0, 0, 0, 0, 0, 1] -- [0, 0, 0, 0, 0, 0, 0]
                ImmutableList.of(1, 0, 1, 0, 1, 0, 1), // [1, 1, 1, 1, 1, 1, 1] -- [0, 0, 0, 0, 0, 0, 0]
                ImmutableList.of(1, 1, 0, 1, 0, 1, 0)  // [1, 1, 1, 1, 1, 1, 1] -- [0, 1, 0, 0, 0, 0, 0]
                // Sums   :     [6, 5, 4, 3, 4, 3, 5]  // [6, 8, 6, 5, 5, 5, 6] -- [0, 3, 3, 2, 2, 2, 2]
                // id < 5 :     [3, 4, 3, 2, 3, 2, 3]  // [3, 5, 4, 3, 3, 3, 3] -- [0, 2, 3, 2, 2, 2, 2]
                // id >= 5:     [3, 1, 1, 1, 1, 1, 2]  // [3, 3, 2, 2, 2, 2, 3] -- [0, 1, 0, 0, 0, 0, 0]
        );

        for (int day = 0; day < 7; day++) {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            for (int id = 0; id < idPresences.size(); id++) {
                if (idPresences.get(id).get(day) == 1) {
                    flamdex.addDocument(new FlamdexDocument.Builder()
                            .addIntTerm("unixtime", new DateTime(2015, 1, day + 1, 0, 0, Constants.DEFAULT_IQL_TIME_ZONE).getMillis() / 1000)
                            .addStringTerm("id", String.valueOf(id))
                            .build()
                    );
                }
            }
            shards.add(new Dataset.DatasetShard("snapshot", "index2015010" + (day + 1), flamdex));
        }

        return new Dataset(shards);
    }
}
