package com.indeed.iql2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.function.Consumer;

import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.ims.client.ImsClient;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.FieldFrequencyCache;
import com.indeed.iql.web.QueryServlet;
import com.indeed.iql2.IQL2Options;
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.iql2.language.commands.Command;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Queries.ParseResult;

import com.indeed.iql2.server.web.servlets.query.CommandValidator;
import com.indeed.iql2.server.web.servlets.query.ExplainQueryExecution;
import com.indeed.util.core.time.DefaultWallClock;

import au.com.bytecode.opencsv.CSVReader;

/**
 * @author rmanvar
 */

public class GroupByTimeValidator {

    public static void main(String[] args) {


        StringTokenizer st;
        int successfulrun = 0;
        int notsuccessful = 0;
        int unknownnotsuccessful = 0;
        int totalran = 0;
        String lastranquery = "";
        try {
            try (BufferedReader br = new BufferedReader(new FileReader("/home/rmanvar/Downloads/groupbytime90diqlwebapp.csv"))) {
                FileOutputStream out = new FileOutputStream("/home/rmanvar/Downloads/unknownfailures90diqlwebapp.txt");
                BufferedWriter bout = new BufferedWriter(new OutputStreamWriter(out));

                FileOutputStream finallyy = new FileOutputStream("/home/rmanvar/Downloads/failedgroupbytimevalidation90diqlwebap.txt");
                BufferedWriter bfinallyy = new BufferedWriter(new OutputStreamWriter(finallyy));

                ImhotepClient client = new ImhotepClient("aus-imozk1.indeed.net", "/imhotep/stage-shardmasters", true);
                final ImsClientInterface imsClient = ImsClient.build("https://squall.indeed.com/iql");
                final ImhotepMetadataCache metadataCache = new ImhotepMetadataCache(imsClient, client, "", new FieldFrequencyCache(null), true);
                metadataCache.updateDatasets();
                final DatasetsMetadata datasetsMetadata = metadataCache.get();

                while (true) {
                    final String line = br.readLine();
                    if (line == null)
                        break;

                    String[] columns = line.split("\t");
                    final String query = columns[0];

                    if (query.contains("...)")) {
                        System.out.println("skipping " + query);
                        continue;
                    }

                    totalran++;
                    if (totalran % 100 == 0) {
                        System.out.println("total ran: " + totalran);
                    }

                    lastranquery = query;

                    try {
                        //public static ParseResult parseQuery(String q, boolean useLegacy, DatasetsMetadata datasetsMetadata, final Set<String> defaultOptions, WallClock clock) {
                        ParseResult parseResult = Queries.parseQuery(query, false, datasetsMetadata, Collections.emptySet(), new DefaultWallClock());
                        Set<String> errors = new HashSet<>();

                        final ExplainQueryExecution explainQueryExecution = new ExplainQueryExecution(
                                datasetsMetadata, new PrintWriter("/home/rmanvar/Downloads/temp.txt"), query, 2, false, new DefaultWallClock(), new IQL2Options());
                        explainQueryExecution.processExplain();
                        //List<Command> commandslist = Queries.queryCommands(parseResult.query, datasetsMetadata);
                        //Set<String> errors = new HashSet<>();
                        //CommandValidator.validate(commandslist, parseResult.query , datasetsMetadata, errors, new HashSet<>());
                        //public static ParseResult parseQuery(String q, boolean useLegacy, DatasetsMetadata datasetsMetadata, final Set<String> defaultOptions, Consumer<String> warn, WallClock clock) {
                        //final List<List<String>> expected = new ArrayList<>();
                        //runQuery(ImhotepClient client, String query, LanguageVersion.version, boolean stream, Options options, Set<String> extraQueryOptions) throws Exception {
                        //QueryServletTestUtils.testIQL2(expected, query );

                        //QueryServletTestUtils.runQuery(client, query, QueryServletTestUtils.LanguageVersion.IQL2, false, QueryServletTestUtils.Options.create(), new HashSet<>(), datasetsMetadata);
                        //queryServlet.query(request, response,  query);

                        //ParseResult parsedResult = Queries.parseQuery(query, false, datasetsMetadata, new Set<String>(), new Consumer<String>() , );

                        // System.out.println("SUCCESS: "+ query);
                        /*Object[] errorslist = errors.toArray();
                        for (int i=0;i<errorslist.length;i++) {
                            if (errorslist[i].toString().contains("You requested a time period")) {
                                System.out.println(query);
                                notsuccessful++;
                            }
                        }*/

                        successfulrun++;
                    } catch (Exception e) {


                        if (e.getMessage().contains("You requested a time period")) {
                            bfinallyy.write(query);
                            bfinallyy.newLine();
                            bfinallyy.flush();
                            notsuccessful++;
                            System.out.println("Failure :" + query);
                        } else if (!e.getMessage().contains("IqlKnownException")) {
                            bout.write(query);
                            bout.newLine();
                            bout.write(e.getMessage());
                            bout.newLine();
                            bout.flush();
                            unknownnotsuccessful++;
                        }

                    }
                }
                System.out.println("success " + successfulrun + "failus " + notsuccessful + " unknownfaile " + unknownnotsuccessful);
                System.out.println("Last run query: " + lastranquery);
                bout.close();
                bfinallyy.close();
            } catch (Exception E) {
                E.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
