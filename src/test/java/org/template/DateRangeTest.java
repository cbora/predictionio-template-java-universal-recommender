package org.template;

import com.fatboyindustrial.gsonjodatime.Converters;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Ignore;
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
   "before": "2015-08-15T11:28:45.114-07:00",
   "after": "2015-08-20T11:28:45.114-07:00"
   }
   */
  @Test
  public void constructorTest() {
    String name = "availabledate";
    String before = "2015-08-15T11:28:45.114-07:00";
    String after = "2015-08-20T11:28:45.114-07:00";
    DateRange dateRange = new DateRange(name, before, after);
    assertEquals(name, dateRange.getName());
    assertEquals(new DateTime(before), dateRange.getBefore());
    assertEquals(new DateTime(after), dateRange.getAfter());
  }

  /**
   *
   "dateRange": {
     "name": "availabledate",
     "before": "2015-08-15T11:28:45.114-07:00",
     "after": "2015-08-20T11:28:45.114-07:00"
   }
   */
  @Test
  public void constructorSerializationTest() {
    String name = "availabledate";
    String before = "2015-08-15T11:28:45.114-07:00";
    String after = "2015-08-20T11:28:45.114-07:00";
    String json = "{\"name\": \"" + name + "\"," +
                  "\"before\": \"" + before + "\"," +
                  "\"after\": \"" + after + "\"}";

    DateRange jsonDateRange = gson.fromJson(json, DateRange.class);
    assertEquals(name, jsonDateRange.getName());
    assertEquals(new DateTime(before), jsonDateRange.getBefore());
    assertEquals(new DateTime(after), jsonDateRange.getAfter());
  }

  /**
   *
   "dateRange": {
   "name": "availabledate",
   "before": "2015-08-15T11:28:45.114-07:00",
   "after": "2015-08-20T11:28:45.114-07:00"
   }
   */
  @Ignore
  public void toStringTest() {
    String name = "availabledate";
    String before = "2015-08-15T11:28:45.114-07:00";
    String after = "2015-08-20T11:28:45.114-07:00";
    String json = "{\"name\": \"" + name + "\"," +
            "\"before\": \"" + before + "\"," +
            "\"after\": \"" + after + "\"}";

    DateRange dateRange = gson.fromJson(json, DateRange.class);
    String result = "DateRange{" +
            ", name= " + name +
            ", before= " + new DateTime(before) +
            ", after= " + new DateTime(after) +
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
            "\"before\": \"" + before + "\"," +
            "\"after\": \"" + after + "\"}";
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
            "\"before\": \"" + before + "\"," +
            "\"after\": \"" + after + "\"}";
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
            "\"before\": \"" + before + "\"," +
            "\"after\": \"" + after + "\"}";
    DateRange dateRange = gson.fromJson(json, DateRange.class);
  }

  // a null field is omitted when printed out as a json through gson
  @Ignore
  public void emptyBeforeTest() {
    String name = "availabledate";
    String before = "2015-08-20T11:28:45.114-07:00";
    String after = "";
    DateRange dateRange = new DateRange(name, before, after);

    String expectedJson = "{\"name\":\"" + name + "\"," +
            "\"before\":\"" + new DateTime(before) + "\"}";
    assertEquals(expectedJson, gson.toJson(dateRange));
  }

  // a null field is omitted when printed out as a json through gson
  @Ignore
  public void emptyAfterTest() {
    String name = "availabledate";
    String before = "";
    String after = "2015-08-20T11:28:45.114-07:00";
    DateRange dateRange = new DateRange(name, before, after);

    String expectedJson = "{\"name\":\"" + name + "\"," +
            "\"after\":\"" + new DateTime(after) + "\"}";
    assertEquals(expectedJson, gson.toJson(dateRange));
    System.out.println(expectedJson);
  }
}
