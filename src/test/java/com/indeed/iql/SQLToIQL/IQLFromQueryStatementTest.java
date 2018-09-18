package com.indeed.iql.SQLToIQL;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class IQLFromQueryStatementTest {

    private final static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("\"yyyy-MM-dd HH:mm:ss\"");

    @Test(expected = IQLFromQueryStatement.IQLFromQueryStatementBuildException.class)
    public void testBothEmptyTimesThrowsOnBuild() {

        new IQLFromQueryStatement.Builder("foo").build();
    }

    @Test(expected = IQLFromQueryStatement.IQLFromQueryStatementBuildException.class)
    public void testEmptyEndTimeThrowsOnBuild() {
        new IQLFromQueryStatement.Builder("foo").addTime(LocalDateTime.now()).build();
    }

    @Test
    public void testQueryFormatsWithEarlierStartTimeCorrectly() {
        final LocalDateTime now = LocalDateTime.now();
        final LocalDateTime later = now.plusMinutes(10L);

        final IQLFromQueryStatement statement =
                new IQLFromQueryStatement.Builder("foo").addTime(now).addTime(later).setAlias("f").build();

        Assert.assertEquals(
                "foo "+now.format(dateFormatter)+" "+later.format(dateFormatter)+" as f",
                statement.toString());
    }

    @Test
    public void testQueryFormatsWithLaterStartTimeCorrectly(){
        final LocalDateTime now = LocalDateTime.now();
        final LocalDateTime later = now.plusDays(1);
        final IQLFromQueryStatement statement= new IQLFromQueryStatement.Builder("a").addTime(later).addTime(now).setAlias("f").build();
        Assert.assertEquals(
                "a "+now.format(dateFormatter)+" "+later.format(dateFormatter)+" as f",
                statement.toString());

    }

    @Test
    public void testQueryFormatsWithoutAlias(){
        final LocalDateTime now = LocalDateTime.now();
        final LocalDateTime later = now.plusMinutes(10L);

        final IQLFromQueryStatement statement =
                new IQLFromQueryStatement.Builder("foo").addTime(now).addTime(later).build();

        Assert.assertEquals(
                "foo "+now.format(dateFormatter)+" "+later.format(dateFormatter),
                statement.toString());

    }


}