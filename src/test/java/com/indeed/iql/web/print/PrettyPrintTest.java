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

package com.indeed.iql.web.print;

import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import org.junit.Assert;
import org.junit.Test;

public class PrettyPrintTest {
    @Test
    public void prettyPrint() throws Exception {
        final DatasetsMetadata datasetsMetadata = AllData.DATASET.getDatasetsMetadata();
        Assert.assertEquals("FROM jobsearch 1d 0d\nWHERE \nGROUP BY \nSELECT count()", PrettyPrint.prettyPrint("from jobsearch 1d 0d", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch 2d 1d\nWHERE \nGROUP BY \nSELECT count()", PrettyPrint.prettyPrint("from jobsearch 2d 1d select count()", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch /* hi */ 2d 1d\nWHERE \nGROUP BY \nSELECT count()", PrettyPrint.prettyPrint("from jobsearch /* hi */ 2d 1d select count()", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch 2d 1d\nWHERE \nGROUP BY \nSELECT /*before*/ count() /* AFTER */", PrettyPrint.prettyPrint("from jobsearch 2d 1d select /*before*/count()/* AFTER */", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch 2d 1d\nWHERE \nGROUP BY time(1week)\nSELECT count()", PrettyPrint.prettyPrint("from jobsearch 2d 1d GROUP BY time(1week) select count()", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch 2d 1d\nWHERE \nGROUP BY time(1d1h)\nSELECT count()", PrettyPrint.prettyPrint("from jobsearch 2d 1d GROUP BY time(1d1h) select count()", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch 2d 1d\nWHERE \nGROUP BY time(90002s)\nSELECT count()", PrettyPrint.prettyPrint("from jobsearch 2d 1d GROUP BY time(90002s) select count()", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch yesterday today\nWHERE \nGROUP BY \nSELECT oji", PrettyPrint.prettyPrint("from jobsearch yesterday today select oji", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch yesterday today\nWHERE \nGROUP BY \nSELECT [oji+ojc]", PrettyPrint.prettyPrint("from jobsearch yesterday today select oji+ojc", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch yesterday today\nWHERE (country=\"us\") (oji=10)\nGROUP BY \nSELECT count()", PrettyPrint.prettyPrint("from jobsearch yesterday today where country:us oji:10 select count()", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch yesterday today\nWHERE ctkrcvd=~\".*\"\nGROUP BY \nSELECT count()", PrettyPrint.prettyPrint("from jobsearch yesterday today where ctkrcvd=~\".*\" select count()", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch yesterday today\nWHERE (escaped=\"stuff\\\"\")\nGROUP BY \nSELECT count()", PrettyPrint.prettyPrint("from jobsearch yesterday today where escaped='stuff\"' select count()", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch 2d 1d\nWHERE (not(country=\"us\")) (not(country=\"us\"))\nGROUP BY \nSELECT count()", PrettyPrint.prettyPrint("from jobsearch 2d 1d WHERE -country:us and -country=us", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch 2d 1d\nWHERE \nGROUP BY \nSELECT count() /  2", PrettyPrint.prettyPrint("from jobsearch 2d 1d select count() /  2", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch 2d 1d\nWHERE \nGROUP BY \nSELECT [1]", PrettyPrint.prettyPrint("from jobsearch 2d 1d select 1", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch 2d 1d\nWHERE \nGROUP BY \nSELECT oji", PrettyPrint.prettyPrint("from jobsearch 2d 1d select oji", true, datasetsMetadata));
        //iql2 queries, contain having clause
        Assert.assertEquals("FROM jobsearch yesterday today\nWHERE \nGROUP BY country HAVING term()=\"us\"\nSELECT count()", PrettyPrint.prettyPrint("from jobsearch yesterday today group by country having term()=\"us\" select count()", false, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch yesterday today\nWHERE \nGROUP BY country HAVING term()=~\"u.*\"\nSELECT count()", PrettyPrint.prettyPrint("from jobsearch yesterday today group by country having term()=~\"u.*\" select count()", false, datasetsMetadata));
        // between differs in Iql1 and Iql2
        Assert.assertEquals("FROM jobsearch yesterday today\nWHERE (((oji>=3) and (oji<=17)))\nGROUP BY \nSELECT counts", PrettyPrint.prettyPrint("from jobsearch yesterday today WHERE between(oji, 3, 17) SELECT counts", true, datasetsMetadata));
        Assert.assertEquals("FROM jobsearch yesterday today\nWHERE between(oji, 3, 17)\nGROUP BY \nSELECT counts", PrettyPrint.prettyPrint("from jobsearch yesterday today WHERE between(oji, 3, 17) SELECT counts", false, datasetsMetadata));
    }

    @Test
    public void stringEscape() throws Exception {
        Assert.assertEquals("abc\\\"def", PrettyPrint.stringEscape("abc\"def"));
        Assert.assertEquals("abc\\ndef", PrettyPrint.stringEscape("abc\ndef"));
    }
}