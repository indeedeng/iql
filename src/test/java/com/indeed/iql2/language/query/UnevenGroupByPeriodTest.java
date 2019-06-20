package com.indeed.iql2.language.query;

import com.indeed.iql.Constants;
import com.indeed.iql2.language.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static com.indeed.iql2.language.query.UnevenGroupByPeriod.MONTH;
import static com.indeed.iql2.language.query.UnevenGroupByPeriod.QUARTER;
import static com.indeed.iql2.language.query.UnevenGroupByPeriod.YEAR;
import static org.junit.Assert.assertEquals;

public class UnevenGroupByPeriodTest {
    private static final DateTimeZone ZONE = Constants.DEFAULT_IQL_TIME_ZONE;

    @Test
    public void fromTimeUnit() {
        assertEquals(YEAR, UnevenGroupByPeriod.fromTimeUnit(TimeUnit.YEAR));
        assertEquals(QUARTER, UnevenGroupByPeriod.fromTimeUnit(TimeUnit.QUARTER));
        assertEquals(MONTH, UnevenGroupByPeriod.fromTimeUnit(TimeUnit.MONTH));
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromTimeUnitException() {
        //noinspection ResultOfMethodCallIgnored
        UnevenGroupByPeriod.fromTimeUnit(TimeUnit.DAY);
    }

    @Test
    public void startOfPeriod() {
        assertEquals(new DateTime(2015, 1, 1, 0, 0, ZONE), YEAR.startOfPeriod(new DateTime(2015, 1, 1, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0, ZONE), YEAR.startOfPeriod(new DateTime(2015, 1, 31, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0, ZONE), YEAR.startOfPeriod(new DateTime(2015, 2, 1, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0, ZONE), YEAR.startOfPeriod(new DateTime(2015, 5, 30, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0, ZONE), YEAR.startOfPeriod(new DateTime(2015, 12, 31, 23, 59, ZONE)));

        assertEquals(new DateTime(2015, 1, 1, 0, 0, ZONE), QUARTER.startOfPeriod(new DateTime(2015, 1, 1, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0, ZONE), QUARTER.startOfPeriod(new DateTime(2015, 1, 31, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0, ZONE), QUARTER.startOfPeriod(new DateTime(2015, 2, 1, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 4, 1, 0, 0, ZONE), QUARTER.startOfPeriod(new DateTime(2015, 5, 30, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 10, 1, 0, 0, ZONE), QUARTER.startOfPeriod(new DateTime(2015, 12, 31, 23, 59, ZONE)));
        // testing moving to month with lower number of days
        assertEquals(new DateTime(2015, 4, 1, 0, 0, ZONE), QUARTER.startOfPeriod(new DateTime(2015, 5, 31, 0, 0, ZONE)));

        assertEquals(new DateTime(2015, 1, 1, 0, 0, ZONE), MONTH.startOfPeriod(new DateTime(2015, 1, 1, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0, ZONE), MONTH.startOfPeriod(new DateTime(2015, 1, 31, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 2, 1, 0, 0, ZONE), MONTH.startOfPeriod(new DateTime(2015, 2, 1, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 5, 1, 0, 0, ZONE), MONTH.startOfPeriod(new DateTime(2015, 5, 30, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 12, 1, 0, 0, ZONE), MONTH.startOfPeriod(new DateTime(2015, 12, 31, 23, 59, ZONE)));
    }

    @Test
    public void endOfPeriod() {
        assertEquals(new DateTime(2016, 1, 1, 0, 0, ZONE), YEAR.endOfPeriod(new DateTime(2015, 1, 1, 0, 0, ZONE)));
        assertEquals(new DateTime(2016, 1, 1, 0, 0, ZONE), YEAR.endOfPeriod(new DateTime(2015, 1, 31, 0, 0, ZONE)));
        assertEquals(new DateTime(2016, 1, 1, 0, 0, ZONE), YEAR.endOfPeriod(new DateTime(2015, 2, 1, 0, 0, ZONE)));
        assertEquals(new DateTime(2016, 1, 1, 0, 0, ZONE), YEAR.endOfPeriod(new DateTime(2015, 5, 30, 0, 0, ZONE)));
        assertEquals(new DateTime(2016, 1, 1, 0, 0, ZONE), YEAR.endOfPeriod(new DateTime(2015, 12, 31, 23, 59, ZONE)));

        assertEquals(new DateTime(2015, 4, 1, 0, 0, ZONE), QUARTER.endOfPeriod(new DateTime(2015, 1, 1, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 4, 1, 0, 0, ZONE), QUARTER.endOfPeriod(new DateTime(2015, 1, 31, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 4, 1, 0, 0, ZONE), QUARTER.endOfPeriod(new DateTime(2015, 2, 1, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 7, 1, 0, 0, ZONE), QUARTER.endOfPeriod(new DateTime(2015, 5, 30, 0, 0, ZONE)));
        assertEquals(new DateTime(2016, 1, 1, 0, 0, ZONE), QUARTER.endOfPeriod(new DateTime(2015, 12, 31, 23, 59, ZONE)));

        assertEquals(new DateTime(2015, 2, 1, 0, 0, ZONE), MONTH.endOfPeriod(new DateTime(2015, 1, 1, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 2, 1, 0, 0, ZONE), MONTH.endOfPeriod(new DateTime(2015, 1, 31, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 3, 1, 0, 0, ZONE), MONTH.endOfPeriod(new DateTime(2015, 2, 1, 0, 0, ZONE)));
        assertEquals(new DateTime(2015, 6, 1, 0, 0, ZONE), MONTH.endOfPeriod(new DateTime(2015, 5, 30, 0, 0, ZONE)));
        assertEquals(new DateTime(2016, 1, 1, 0, 0, ZONE), MONTH.endOfPeriod(new DateTime(2015, 12, 31, 23, 59, ZONE)));
    }

    @Test
    public void plusPeriods() {
        assertEquals(new DateTime(2015, 1, 1, 0, 0, ZONE), YEAR.plusPeriods(new DateTime(2015, 1, 1, 0, 0, ZONE), 0));
        assertEquals(new DateTime(2016, 1, 1, 0, 0, ZONE), YEAR.plusPeriods(new DateTime(2015, 1, 1, 0, 0, ZONE), 1));
        assertEquals(new DateTime(2020, 1, 1, 0, 0, ZONE), YEAR.plusPeriods(new DateTime(2015, 1, 1, 0, 0, ZONE), 5));
        assertEquals(new DateTime(2025, 1, 1, 0, 0, ZONE), YEAR.plusPeriods(new DateTime(2015, 1, 1, 0, 0, ZONE), 10));
        assertEquals(new DateTime(2010, 1, 1, 0, 0, ZONE), YEAR.plusPeriods(new DateTime(2015, 1, 1, 0, 0, ZONE), -5));

        assertEquals(new DateTime(2015, 1, 1, 0, 0, ZONE), QUARTER.plusPeriods(new DateTime(2015, 1, 1, 0, 0, ZONE), 0));
        assertEquals(new DateTime(2015, 4, 1, 0, 0, ZONE), QUARTER.plusPeriods(new DateTime(2015, 1, 1, 0, 0, ZONE), 1));
        assertEquals(new DateTime(2014, 7, 1, 0, 0, ZONE), QUARTER.plusPeriods(new DateTime(2015, 1, 1, 0, 0, ZONE), -2));
        assertEquals(new DateTime(2015, 4, 1, 0, 0, ZONE), QUARTER.plusPeriods(new DateTime(2015, 1, 1, 0, 0, ZONE), 1));
        assertEquals(new DateTime(2018, 10, 1, 0, 0, ZONE), QUARTER.plusPeriods(new DateTime(2015, 10, 1, 0, 0, ZONE), 12));
        assertEquals(new DateTime(2020, 7, 1, 0, 0, ZONE), QUARTER.plusPeriods(new DateTime(2020, 4, 1, 0, 0, ZONE), 1));

        assertEquals(new DateTime(2015, 1, 1, 0, 0, ZONE), MONTH.plusPeriods(new DateTime(2015, 1, 1, 0, 0, ZONE), 0));
        assertEquals(new DateTime(2015, 2, 1, 0, 0, ZONE), MONTH.plusPeriods(new DateTime(2015, 1, 1, 0, 0, ZONE), 1));
        assertEquals(new DateTime(2015, 11, 1, 0, 0, ZONE), MONTH.plusPeriods(new DateTime(2015, 1, 1, 0, 0, ZONE), 10));
        assertEquals(new DateTime(2025, 1, 1, 0, 0, ZONE), MONTH.plusPeriods(new DateTime(2015, 1, 1, 0, 0, ZONE), 120));
        assertEquals(new DateTime(2015, 3, 1, 0, 0, ZONE), MONTH.plusPeriods(new DateTime(2015, 2, 1, 0, 0, ZONE), 1));
        assertEquals(new DateTime(2015, 4, 1, 0, 0, ZONE), MONTH.plusPeriods(new DateTime(2015, 3, 1, 0, 0, ZONE), 1));
        assertEquals(new DateTime(2015, 6, 1, 0, 0, ZONE), MONTH.plusPeriods(new DateTime(2015, 5, 1, 0, 0, ZONE), 1));
    }

    @Test
    public void periodsBetween() {
        assertEquals(0, YEAR.periodsBetween(new DateTime(2015, 1, 1, 0, 0, ZONE), new DateTime(2015, 1, 1, 0, 0, ZONE)));
        assertEquals(1, YEAR.periodsBetween(new DateTime(2015, 1, 1, 0, 0, ZONE), new DateTime(2016, 1, 1, 0, 0, ZONE)));
        assertEquals(-1, YEAR.periodsBetween(new DateTime(2016, 1, 1, 0, 0, ZONE), new DateTime(2015, 1, 1, 0, 0, ZONE)));
        assertEquals(80, YEAR.periodsBetween(new DateTime(2015, 1, 1, 0, 0, ZONE), new DateTime(2095, 1, 1, 0, 0, ZONE)));

        assertEquals(0, QUARTER.periodsBetween(new DateTime(2015, 1, 1, 0, 0, ZONE), new DateTime(2015, 1, 1, 0, 0, ZONE)));
        assertEquals(1, QUARTER.periodsBetween(new DateTime(2015, 1, 1, 0, 0, ZONE), new DateTime(2015, 4, 1, 0, 0, ZONE)));
        assertEquals(8, QUARTER.periodsBetween(new DateTime(2015, 1, 1, 0, 0, ZONE), new DateTime(2017, 1, 1, 0, 0, ZONE)));
        assertEquals(-8, QUARTER.periodsBetween(new DateTime(2015, 1, 1, 0, 0, ZONE), new DateTime(2013, 1, 1, 0, 0, ZONE)));

        assertEquals(0, MONTH.periodsBetween(new DateTime(2015, 1, 1, 0, 0, ZONE), new DateTime(2015, 1, 1, 0, 0, ZONE)));
        assertEquals(1, MONTH.periodsBetween(new DateTime(2015, 1, 1, 0, 0, ZONE), new DateTime(2015, 2, 1, 0, 0, ZONE)));
        assertEquals(-1, MONTH.periodsBetween(new DateTime(2015, 1, 1, 0, 0, ZONE), new DateTime(2014, 12, 1, 0, 0, ZONE)));
        assertEquals(120, MONTH.periodsBetween(new DateTime(2015, 1, 1, 0, 0, ZONE), new DateTime(2025, 1, 1, 0, 0, ZONE)));
    }
}