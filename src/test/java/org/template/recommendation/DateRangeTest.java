package org.template.recommendation;

import com.fatboyindustrial.gsonjodatime.Converters;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *  Unit test for DateRange class
 *  Also tests capacity of DateRangeTypeAdapterFactory as a consequence
 */
public class DateRangeTest {

  private Gson gson;

  @Before
  public void init() {
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapterFactory(new DateRangeTypeAdapterFactory());
    this.gson = Converters.registerDateTime(builder).create();
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
  public void constructorTest() {
    String name = "availabledate";
    String before = "2015-08-15T11:28:45.114-07:00";
    String after = "2015-08-20T11:28:45.114-07:00";
    DateRange dateRange = new DateRange(name, before, after);
    assertEquals(name, dateRange.getName());
    assertEquals(new DateTime(before), dateRange.getBeforeDate());
    assertEquals(new DateTime(after), dateRange.getAfterDate());
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
  public void constructorSerializationTest() {
    String name = "availabledate";
    String before = "2015-08-15T11:28:45.114-07:00";
    String after = "2015-08-20T11:28:45.114-07:00";
    String json = "{\"name\": \"" + name + "\"," +
                  "\"beforeDate\": \"" + before + "\"," +
                  "\"afterDate\": \"" + after + "\"}";

    DateRange jsonDateRange = gson.fromJson(json, DateRange.class);
    assertEquals(name, jsonDateRange.getName());
    assertEquals(new DateTime(before), jsonDateRange.getBeforeDate());
    assertEquals(new DateTime(after), jsonDateRange.getAfterDate());
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
    String json = "{\"name\": \"" + name + "\"," +
            "\"beforeDate\": \"" + before + "\"," +
            "\"afterDate\": \"" + after + "\"}";

    DateRange dateRange = gson.fromJson(json, DateRange.class);
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
  public void invalidBeforeSerializationTest() {
    String name = "availabledate";
    String before = "notavaliddate";
    String after = "2015-08-20T11:28:45.114-07:00";
    String json = "{\"name\": \"" + name + "\"," +
            "\"beforeDate\": \"" + before + "\"," +
            "\"afterDate\": \"" + after + "\"}";
    DateRange dateRange = gson.fromJson(json, DateRange.class);
  }

  @Test(expected=IllegalArgumentException.class)
  public void invalidAfterTest() {
    String name = "availabledate";
    String before = "2015-08-20T11:28:45.114-07:00";
    String after = "notavaliddate";
    DateRange dateRange = new DateRange(name, before, after);
  }

  @Test(expected=IllegalArgumentException.class)
  public void invalidAfterSerializationTest() {
    String name = "availabledate";
    String before = "2015-08-20T11:28:45.114-07:00";
    String after = "notavaliddate";
    String json = "{\"name\": \"" + name + "\"," +
            "\"beforeDate\": \"" + before + "\"," +
            "\"afterDate\": \"" + after + "\"}";
    DateRange dateRange = gson.fromJson(json, DateRange.class);
  }

  @Test(expected=IllegalArgumentException.class)
  public void emptyBeforeAfterDatesTest() {
    String name = "availabledate";
    String before = "";
    String after = "";
    DateRange dateRange = new DateRange(name, before, after);
  }

  @Test(expected=IllegalArgumentException.class)
  public void emptyBeforeAfterDatesSerializationTest() {
    String name = "availabledate";
    String before = "";
    String after = "";
    String json = "{\"name\": \"" + name + "\"," +
            "\"beforeDate\": \"" + before + "\"," +
            "\"afterDate\": \"" + after + "\"}";
    DateRange dateRange = gson.fromJson(json, DateRange.class);
  }

  // a null field is omitted when printed out as a json through gson
  @Test
  public void emptyBeforeTest() {
    String name = "availabledate";
    String before = "2015-08-20T11:28:45.114-07:00";
    String after = "";
    DateRange dateRange = new DateRange(name, before, after);

    String expectedJson = "{\"name\":\"" + name + "\"," +
            "\"beforeDate\":\"" + new DateTime(before) + "\"}";
    assertEquals(expectedJson, gson.toJson(dateRange));
  }

  // a null field is omitted when printed out as a json through gson
  @Test
  public void emptyAfterTest() {
    String name = "availabledate";
    String before = "";
    String after = "2015-08-20T11:28:45.114-07:00";
    DateRange dateRange = new DateRange(name, before, after);

    String expectedJson = "{\"name\":\"" + name + "\"," +
            "\"afterDate\":\"" + new DateTime(after) + "\"}";
    assertEquals(expectedJson, gson.toJson(dateRange));
  }
}
