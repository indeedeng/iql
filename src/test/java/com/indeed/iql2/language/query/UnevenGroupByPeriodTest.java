package com.indeed.iql2.language.query;

import com.indeed.iql2.language.TimeUnit;
import org.joda.time.DateTime;
import org.junit.Test;

import static com.indeed.iql2.language.query.UnevenGroupByPeriod.MONTH;
import static com.indeed.iql2.language.query.UnevenGroupByPeriod.QUARTER;
import static com.indeed.iql2.language.query.UnevenGroupByPeriod.YEAR;
import static org.junit.Assert.assertEquals;

public class UnevenGroupByPeriodTest {
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
        assertEquals(new DateTime(2015, 1, 1, 0, 0), YEAR.startOfPeriod(new DateTime(2015, 1, 1, 0, 0)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0), YEAR.startOfPeriod(new DateTime(2015, 1, 31, 0, 0)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0), YEAR.startOfPeriod(new DateTime(2015, 2, 1, 0, 0)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0), YEAR.startOfPeriod(new DateTime(2015, 5, 30, 0, 0)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0), YEAR.startOfPeriod(new DateTime(2015, 12, 31, 23, 59)));

        assertEquals(new DateTime(2015, 1, 1, 0, 0), QUARTER.startOfPeriod(new DateTime(2015, 1, 1, 0, 0)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0), QUARTER.startOfPeriod(new DateTime(2015, 1, 31, 0, 0)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0), QUARTER.startOfPeriod(new DateTime(2015, 2, 1, 0, 0)));
        assertEquals(new DateTime(2015, 4, 1, 0, 0), QUARTER.startOfPeriod(new DateTime(2015, 5, 30, 0, 0)));
        assertEquals(new DateTime(2015, 10, 1, 0, 0), QUARTER.startOfPeriod(new DateTime(2015, 12, 31, 23, 59)));
        // testing moving to month with lower number of days
        assertEquals(new DateTime(2015, 4, 1, 0, 0), QUARTER.startOfPeriod(new DateTime(2015, 5, 31, 0, 0)));

        assertEquals(new DateTime(2015, 1, 1, 0, 0), MONTH.startOfPeriod(new DateTime(2015, 1, 1, 0, 0)));
        assertEquals(new DateTime(2015, 1, 1, 0, 0), MONTH.startOfPeriod(new DateTime(2015, 1, 31, 0, 0)));
        assertEquals(new DateTime(2015, 2, 1, 0, 0), MONTH.startOfPeriod(new DateTime(2015, 2, 1, 0, 0)));
        assertEquals(new DateTime(2015, 5, 1, 0, 0), MONTH.startOfPeriod(new DateTime(2015, 5, 30, 0, 0)));
        assertEquals(new DateTime(2015, 12, 1, 0, 0), MONTH.startOfPeriod(new DateTime(2015, 12, 31, 23, 59)));
    }

    @Test
    public void endOfPeriod() {
        assertEquals(new DateTime(2016, 1, 1, 0, 0), YEAR.endOfPeriod(new DateTime(2015, 1, 1, 0, 0)));
        assertEquals(new DateTime(2016, 1, 1, 0, 0), YEAR.endOfPeriod(new DateTime(2015, 1, 31, 0, 0)));
        assertEquals(new DateTime(2016, 1, 1, 0, 0), YEAR.endOfPeriod(new DateTime(2015, 2, 1, 0, 0)));
        assertEquals(new DateTime(2016, 1, 1, 0, 0), YEAR.endOfPeriod(new DateTime(2015, 5, 30, 0, 0)));
        assertEquals(new DateTime(2016, 1, 1, 0, 0), YEAR.endOfPeriod(new DateTime(2015, 12, 31, 23, 59)));

        assertEquals(new DateTime(2015, 4, 1, 0, 0), QUARTER.endOfPeriod(new DateTime(2015, 1, 1, 0, 0)));
        assertEquals(new DateTime(2015, 4, 1, 0, 0), QUARTER.endOfPeriod(new DateTime(2015, 1, 31, 0, 0)));
        assertEquals(new DateTime(2015, 4, 1, 0, 0), QUARTER.endOfPeriod(new DateTime(2015, 2, 1, 0, 0)));
        assertEquals(new DateTime(2015, 7, 1, 0, 0), QUARTER.endOfPeriod(new DateTime(2015, 5, 30, 0, 0)));
        assertEquals(new DateTime(2016, 1, 1, 0, 0), QUARTER.endOfPeriod(new DateTime(2015, 12, 31, 23, 59)));

        assertEquals(new DateTime(2015, 2, 1, 0, 0), MONTH.endOfPeriod(new DateTime(2015, 1, 1, 0, 0)));
        assertEquals(new DateTime(2015, 2, 1, 0, 0), MONTH.endOfPeriod(new DateTime(2015, 1, 31, 0, 0)));
        assertEquals(new DateTime(2015, 3, 1, 0, 0), MONTH.endOfPeriod(new DateTime(2015, 2, 1, 0, 0)));
        assertEquals(new DateTime(2015, 6, 1, 0, 0), MONTH.endOfPeriod(new DateTime(2015, 5, 30, 0, 0)));
        assertEquals(new DateTime(2016, 1, 1, 0, 0), MONTH.endOfPeriod(new DateTime(2015, 12, 31, 23, 59)));
    }

    @Test
    public void plusPeriods() {
        assertEquals(new DateTime(2015, 1, 1, 0, 0), YEAR.plusPeriods(new DateTime(2015, 1, 1, 0, 0), 0));
        assertEquals(new DateTime(2016, 1, 1, 0, 0), YEAR.plusPeriods(new DateTime(2015, 1, 1, 0, 0), 1));
        assertEquals(new DateTime(2020, 1, 1, 0, 0), YEAR.plusPeriods(new DateTime(2015, 1, 1, 0, 0), 5));
        assertEquals(new DateTime(2025, 1, 1, 0, 0), YEAR.plusPeriods(new DateTime(2015, 1, 1, 0, 0), 10));
        assertEquals(new DateTime(2010, 1, 1, 0, 0), YEAR.plusPeriods(new DateTime(2015, 1, 1, 0, 0), -5));

        assertEquals(new DateTime(2015, 1, 1, 0, 0), QUARTER.plusPeriods(new DateTime(2015, 1, 1, 0, 0), 0));
        assertEquals(new DateTime(2015, 4, 1, 0, 0), QUARTER.plusPeriods(new DateTime(2015, 1, 1, 0, 0), 1));
        assertEquals(new DateTime(2014, 7, 1, 0, 0), QUARTER.plusPeriods(new DateTime(2015, 1, 1, 0, 0), -2));
        assertEquals(new DateTime(2015, 4, 1, 0, 0), QUARTER.plusPeriods(new DateTime(2015, 1, 1, 0, 0), 1));
        assertEquals(new DateTime(2018, 10, 1, 0, 0), QUARTER.plusPeriods(new DateTime(2015, 10, 1, 0, 0), 12));
        assertEquals(new DateTime(2020, 7, 1, 0, 0), QUARTER.plusPeriods(new DateTime(2020, 4, 1, 0, 0), 1));

        assertEquals(new DateTime(2015, 1, 1, 0, 0), MONTH.plusPeriods(new DateTime(2015, 1, 1, 0, 0), 0));
        assertEquals(new DateTime(2015, 2, 1, 0, 0), MONTH.plusPeriods(new DateTime(2015, 1, 1, 0, 0), 1));
        assertEquals(new DateTime(2015, 11, 1, 0, 0), MONTH.plusPeriods(new DateTime(2015, 1, 1, 0, 0), 10));
        assertEquals(new DateTime(2025, 1, 1, 0, 0), MONTH.plusPeriods(new DateTime(2015, 1, 1, 0, 0), 120));
        assertEquals(new DateTime(2015, 3, 1, 0, 0), MONTH.plusPeriods(new DateTime(2015, 2, 1, 0, 0), 1));
        assertEquals(new DateTime(2015, 4, 1, 0, 0), MONTH.plusPeriods(new DateTime(2015, 3, 1, 0, 0), 1));
        assertEquals(new DateTime(2015, 6, 1, 0, 0), MONTH.plusPeriods(new DateTime(2015, 5, 1, 0, 0), 1));
    }

    @Test
    public void periodsBetween() {
        assertEquals(0, YEAR.periodsBetween(new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 1, 0, 0)));
        assertEquals(1, YEAR.periodsBetween(new DateTime(2015, 1, 1, 0, 0), new DateTime(2016, 1, 1, 0, 0)));
        assertEquals(-1, YEAR.periodsBetween(new DateTime(2016, 1, 1, 0, 0), new DateTime(2015, 1, 1, 0, 0)));
        assertEquals(80, YEAR.periodsBetween(new DateTime(2015, 1, 1, 0, 0), new DateTime(2095, 1, 1, 0, 0)));

        assertEquals(0, QUARTER.periodsBetween(new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 1, 0, 0)));
        assertEquals(1, QUARTER.periodsBetween(new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 4, 1, 0, 0)));
        assertEquals(8, QUARTER.periodsBetween(new DateTime(2015, 1, 1, 0, 0), new DateTime(2017, 1, 1, 0, 0)));
        assertEquals(-8, QUARTER.periodsBetween(new DateTime(2015, 1, 1, 0, 0), new DateTime(2013, 1, 1, 0, 0)));

        assertEquals(0, MONTH.periodsBetween(new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 1, 0, 0)));
        assertEquals(1, MONTH.periodsBetween(new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 2, 1, 0, 0)));
        assertEquals(-1, MONTH.periodsBetween(new DateTime(2015, 1, 1, 0, 0), new DateTime(2014, 12, 1, 0, 0)));
        assertEquals(120, MONTH.periodsBetween(new DateTime(2015, 1, 1, 0, 0), new DateTime(2025, 1, 1, 0, 0)));
    }
}