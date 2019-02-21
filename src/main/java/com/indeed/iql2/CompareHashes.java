package com.indeed.iql2;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.base.Preconditions;

import java.io.FileReader;
import java.io.IOException;

public class CompareHashes {
    private CompareHashes() {
    }

    public static void main(String[] args) throws IOException {
        final String input1 = args[0];
        final String input2 = args[1];

        try (CSVReader csv1 = new CSVReader(new FileReader(input1), ',', '"', '\\');
             CSVReader csv2 = new CSVReader(new FileReader(input2), ',', '"', '\\')) {

            int count = 0;

            while (true) {
                final String[] line1 = csv1.readNext();
                final String[] line2 = csv2.readNext();

                Preconditions.checkState((line1 == null) == (line2 == null));
                if (line1 == null) {
                    break;
                }

                Preconditions.checkState(line1[0].equals(line2[0]));
                final String query = line1[0];

                if (query.contains("SAMPLE(") || query.contains("sample(")) {
                    continue;
                }

                if (!line1[1].equals(line2[1])) {
                    System.out.println("Query(" + count + "): " + query);
                    System.out.println("Hash1 = " + line1[1]);
                    System.out.println("Hash2 = " + line2[1]);
                }

                count += 1;
            }

        }
    }
}
