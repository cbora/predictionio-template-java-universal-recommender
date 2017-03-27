package org.template.recommendation;

import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *  Unit test for DateRange class
 */
public class DateRangeTest {

  /**
   *
   "dateRange": {
     "name": "availabledate",
     "beforeDate": "2015-08-15T11:28:45.114-07:00",
     "afterDate": "2015-08-20T11:28:45.114-07:00"
   }
   */
  @Test
  public void constructorTest() {
    String name = "availabledate";
    String before = "2015-08-15T11:28:45.114-07:00";
    String after = "2015-08-20T11:28:45.114-07:00";
    DateRange dateRange = new DateRange(name, before, after);
    assertEquals(name, dateRange.getName());
    assertEquals(before, dateRange.getBefore());
    assertEquals(after, dateRange.getAfter());
  }

  /**
   *
   "dateRange": {
   "name": "availabledate",
   "beforeDate": "2015-08-15T11:28:45.114-07:00",
   "afterDate": "2015-08-20T11:28:45.114-07:00"
   }
   */
  @Test
  public void toStringTest() {
    String name = "availabledate";
    String before = "2015-08-15T11:28:45.114-07:00";
    String after = "2015-08-20T11:28:45.114-07:00";
    DateRange dateRange = new DateRange(name, before, after);
    String result = "DateRange{" +
            ", name= " + name +
            ", before= " + before +
            ", after= " + after +
            '}';
    assertEquals(result, dateRange.toString());
  }

  @Test(expected=IllegalArgumentException.class)
  public void invalidBeforeTest() {
    String name = "availabledate";
    String before = "notavaliddate";
    String after = "2015-08-20T11:28:45.114-07:00";
    DateRange dateRange = new DateRange(name, before, after);
  }

  @Test(expected=IllegalArgumentException.class)
  public void invalidAfterTest() {
    String name = "availabledate";
    String before = "2015-08-20T11:28:45.114-07:00";
    String after = "notavaliddate";
    DateRange dateRange = new DateRange(name, before, after);
  }

  @Test(expected=IllegalArgumentException.class)
  public void emptyBeforeAfterDatesTest() {
    String name = "availabledate";
    String before = "";
    String after = "";
    DateRange dateRange = new DateRange(name, before, after);
  }

  @Test
  public void emptyBeforeTest() {
    String name = "availabledate";
    String before = "2015-08-20T11:28:45.114-07:00";
    String after = "";
    DateRange dateRange = new DateRange(name, before, after);
  }

  @Test
  public void emptyAfterTest() {
    String name = "availabledate";
    String before = "";
    String after = "2015-08-20T11:28:45.114-07:00";
    DateRange dateRange = new DateRange(name, before, after);
  }

  @Test
  public void getBeforeDateTimeTest() {
    String name = "availabledate";
    String before = "2015-08-15T11:28:45.114-07:00";
    String after = "2015-08-20T11:28:45.114-07:00";
    DateRange dateRange = new DateRange(name, before, after);
    assertEquals(new DateTime(before), dateRange.getBeforeDateTime());
  }

  @Test
  public void getAfterDateTimeTest() {
    String name = "availabledate";
    String before = "2015-08-15T11:28:45.114-07:00";
    String after = "2015-08-20T11:28:45.114-07:00";
    DateRange dateRange = new DateRange(name, before, after);
    assertEquals(new DateTime(after), dateRange.getAfterDateTime());
  }

  @Test(expected=IllegalArgumentException.class)
  public void invalidGetBeforeDateTimeTest() {
    String name = "availabledate";
    String before = "";
    String after = "2015-08-20T11:28:45.114-07:00";
    DateRange dateRange = new DateRange(name, before, after);
    DateTime b =  dateRange.getBeforeDateTime();
  }

  @Test(expected=IllegalArgumentException.class)
  public void invalidGetAfterDateTimeTest() {
    String name = "availabledate";
    String before = "2015-08-15T11:28:45.114-07:00";
    String after = "";
    DateRange dateRange = new DateRange(name, before, after);
    DateTime b =  dateRange.getAfterDateTime();
  }
}
