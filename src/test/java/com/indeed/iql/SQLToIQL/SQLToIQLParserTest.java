package com.indeed.iql.SQLToIQL;

import org.apache.log4j.BasicConfigurator;
import org.junit.Assert;
import org.junit.Test;


public class SQLToIQLParserTest {

    @Test
    public void convertSqlToIql() {

        BasicConfigurator.configure();

        final String sql =
                "select count(*)\n" +
                        "from a, b as a1\n" +
                        "where a.time > \"2015-01-06 00:00:00\" " +
                        "AND a.time < \"2018-01-06 00:00:00\" " +
                        "AND b.time < \"2015-01-06 00:00:00\" " +
                        "AND b.time > \"2011-05-08 00:00:00\" " +
                        "AND a.rcv = 'jsv' " +
                        "AND a.grp = 'smartphone' " +
                        "AND b.grp != 'privileged' " +
                        "AND b.grp = 'mobacmenavbacktotst0';";

        final String expected =
                "from a \"2015-01-06 00:00:00\" \"2018-01-06 00:00:00\", " +
                        "b \"2011-05-08 00:00:00\" \"2015-01-06 00:00:00\" as a1 " +
                        "where a.rcv = 'jsv' " +
                        "AND a.grp = 'smartphone' " +
                        "AND b.grp != 'privileged' " +
                        "AND b.grp = 'mobacmenavbacktotst0'";

        SQLToIQLParser sqlToIQLParser= new SQLToIQLParser(new AntlrParserGenerator());

        Assert.assertEquals(expected.trim(), sqlToIQLParser.parse(sql));

    }

    @Test
    public void basicConvert(){
        final String sql=" select count(*)\n" +
                "from mobviewjob\n" +
                "where mobviewjob.time>\"2014-11-21 00:00:00\" " +
                "AND mobviewjob.time <\"2015-01-06 00:00:00\" " +
                "AND rcv = 'jsv' AND grp = 'smartphone' " +
                "AND grp != 'privileged' " +
                "AND grp = 'mobacmenavbacktotst0';\n";

        final String iql="from mobviewjob \"2014-11-21 00:00:00\" \"2015-01-06 00:00:00\"" +
                " where rcv = 'jsv' AND grp = 'smartphone' " +
                "AND grp != 'privileged' AND grp = 'mobacmenavbacktotst0'";

        SQLToIQLParser sqlToIQLParser= new SQLToIQLParser(new AntlrParserGenerator());

        Assert.assertEquals(iql.trim(), sqlToIQLParser.parse(sql));

    }
    @Test
    public void inputWithAliasing(){
        final String sql="select count(*)\n" +
                "from mobviewjob as mob\n" +
                "where mob.time>\"2014-11-21 00:00:00\" " +
                "AND mob.time <\"2015-01-06 00:00:00\" " +
                "AND mob.rcv = 'jsv' AND mob.grp = 'smartphone' " +
                "AND mob.grp != 'privileged' AND mob.grp = 'mobacmenavbacktotst0';\n";

        final String iql="from mobviewjob \"2014-11-21 00:00:00\" \"2015-01-06 00:00:00\"" +
                " as mob where mob.rcv = 'jsv' " +
                "AND mob.grp = 'smartphone' " +
                "AND mob.grp != 'privileged' " +
                "AND mob.grp = 'mobacmenavbacktotst0'";

        SQLToIQLParser sqlToIQLParser= new SQLToIQLParser(new AntlrParserGenerator());

        Assert.assertEquals(iql.trim(), sqlToIQLParser.parse(sql));
    }

    @Test(expected = IQLFromQueryStatement.IQLFromQueryStatementBuildException.class)
    public void inputWithoutTime(){
            final String sql="select count(*)\n" +
                "from mobviewjob\n" +
                "where rcv = 'jsv' AND grp = 'smartphone'" +
                " AND grp != 'privileged' AND grp = 'mobacmenavbacktotst0';\n";

        SQLToIQLParser sqlToIQLParser= new SQLToIQLParser(new AntlrParserGenerator());

        sqlToIQLParser.parse(sql);
    }

    @Test(expected = IQLOperator.UnknownOperatorException.class)
    public void invalidOperator(){
        final String sql="select count(*)\n" +
                "from mobviewjob\n" +
                "where mobviewjob.time>\"2014-11-21 00:00:00\" " +
                "AND mobviewjob.time <\"2015-01-06 00:00:00\" " +
                "AND rcv <> 'jsv' AND grp = 'smartphone' " +
                "AND grp != 'privileged' " +
                "AND grp = 'mobacmenavbacktotst0';\n";

        SQLToIQLParser sqlToIQLParser= new SQLToIQLParser(new AntlrParserGenerator());

        sqlToIQLParser.parse(sql);

    }

    @Test
    public void greaterThanOperator(){
        final String sql="select count(*)\n" +
                "from mobviewjob\n" +
                "where mobviewjob.time>\"2014-11-21 00:00:00\" " +
                "AND mobviewjob.time <\"2015-01-06 00:00:00\" " +
                "AND rcv > 'jsv' AND grp = 'smartphone' " +
                "AND grp != 'privileged' " +
                "AND grp = 'mobacmenavbacktotst0';\n";

        final String iql="from mobviewjob \"2014-11-21 00:00:00\" \"2015-01-06 00:00:00\"" +
                " as mob where rcv > 'jsv' " +
                "AND grp = 'smartphone' " +
                "AND grp != 'privileged' " +
                "AND grp = 'mobacmenavbacktotst0'";

        SQLToIQLParser sqlToIQLParser= new SQLToIQLParser(new AntlrParserGenerator());

        sqlToIQLParser.parse(sql);

    }



    @Test(expected = NotImplementedException.class)
    public void notImplementedFunction(){
        final String sql="\n" +
                "select count(*)\n" +
                "from mobviewjob.recent;\n";

        SQLToIQLParser sqlToIQLParser= new SQLToIQLParser(new AntlrParserGenerator());

        sqlToIQLParser.parse(sql);
    }

    @Test(expected = RawSQLQueryInfoHandler.TableNumberExceedMax.class)
    public void tableNumberExceedMax(){
        final String sql="select count(*)\n" +
                "from a,b,c,d,e,f,g,h,i\n" +
                "where a.time>\"2014-11-21 00:00:00\" AND a.time <\"2015-01-06 00:00:00\" " +
                "AND  b.time>\"2014-11-21 00:00:00\" AND b.time <\"2015-01-06 00:00:00\" " +
                "AND  c.time>\"2014-11-21 00:00:00\" AND c.time <\"2015-01-06 00:00:00\" " +
                "AND  d.time>\"2014-11-21 00:00:00\" AND d.time <\"2015-01-06 00:00:00\" " +
                "AND  e.time>\"2014-11-21 00:00:00\" AND e.time <\"2015-01-06 00:00:00\" " +
                "AND  f.time>\"2014-11-21 00:00:00\" AND f.time <\"2015-01-06 00:00:00\" " +
                "AND  g.time>\"2014-11-21 00:00:00\" AND g.time <\"2015-01-06 00:00:00\" " +
                "AND  h.time>\"2014-11-21 00:00:00\" AND h.time <\"2015-01-06 00:00:00\" " +
                "AND  i.time>\"2014-11-21 00:00:00\" AND i.time <\"2015-01-06 00:00:00\";\n";

        SQLToIQLParser sqlToIQLParser= new SQLToIQLParser(new AntlrParserGenerator());

        sqlToIQLParser.parse(sql);

    }
    @Test
    public void queryWithOnlyTableAndTime(){
        final String sql="select count(*)\n" +
                "from mobviewjob\n" +
                "where mobviewjob.time>\"2014-11-21 00:00:00\" " +
                "AND mobviewjob.time <\"2015-01-06 00:00:00\" ";
        final String iql ="from mobviewjob \"2014-11-21 00:00:00\" \"2015-01-06 00:00:00\" ";

        SQLToIQLParser sqlToIQLParser= new SQLToIQLParser(new AntlrParserGenerator());

        Assert.assertEquals(iql.trim(), sqlToIQLParser.parse(sql));
    }

    @Test
    public void queryWithOneGroupBy(){
        final String sql=" select count(*)\n" +
                "from mobviewjob\n" +
                "where mobviewjob.time>\"2014-11-21 00:00:00\" " +
                "AND mobviewjob.time <\"2015-01-06 00:00:00\" " +
                "AND rcv = 'jsv' AND grp = 'smartphone' " +
                "AND grp != 'privileged' " +
                "AND grp = 'mobacmenavbacktotst0' " +
                "group by rcv;\n";

        final String iql="from mobviewjob \"2014-11-21 00:00:00\" \"2015-01-06 00:00:00\"" +
                " where rcv = 'jsv' AND grp = 'smartphone' " +
                "AND grp != 'privileged' AND grp = 'mobacmenavbacktotst0' " +
                "group by rcv";

        SQLToIQLParser sqlToIQLParser= new SQLToIQLParser(new AntlrParserGenerator());

        Assert.assertEquals(iql.trim(), sqlToIQLParser.parse(sql));
    }

    @Test
    public void queryWithMoreThanOneGroupBy(){
        final String sql=" select count(*)\n" +
                "from mobviewjob\n" +
                "where mobviewjob.time>\"2014-11-21 00:00:00\" " +
                "AND mobviewjob.time <\"2015-01-06 00:00:00\" " +
                "AND rcv = 'jsv' AND grp = 'smartphone' " +
                "AND grp != 'privileged' " +
                "AND grp = 'mobacmenavbacktotst0' " +
                "group by rcv, grp;\n";

        final String iql="from mobviewjob \"2014-11-21 00:00:00\" \"2015-01-06 00:00:00\"" +
                " where rcv = 'jsv' AND grp = 'smartphone' " +
                "AND grp != 'privileged' AND grp = 'mobacmenavbacktotst0' " +
                "group by rcv, grp";

        SQLToIQLParser sqlToIQLParser= new SQLToIQLParser(new AntlrParserGenerator());

        Assert.assertEquals(iql.trim(), sqlToIQLParser.parse(sql));
    }

}