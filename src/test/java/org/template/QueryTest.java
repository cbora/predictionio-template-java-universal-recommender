package org.template;

import com.fatboyindustrial.gsonjodatime.Converters;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for Query class
 */
public class QueryTest {

  private Gson gson;

  @Before
  public void init() {
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapterFactory(new DateRangeTypeAdapterFactory());
    this.gson = Converters.registerDateTime(builder).create();
  }

  @Test
  public void simplePersonalizedQueryTest() {
    String queryJson = "{ \"user\": \"xyz\"}";
    Query query = gson.fromJson(queryJson, Query.class);
    assertEquals("xyz", query.getUser());
  }

  @Test
  public void simpleSimilarItemsQueryTest() {
    String queryJson = "{ \"item\": \"53454543513\"}";
    Query query = gson.fromJson(queryJson, Query.class);
    assertEquals("53454543513", query.getItem());
  }

  @Test
  public void popularItemsQueryTest() {
    String queryJson = "{}";
    Query query = gson.fromJson(queryJson, Query.class);
    assertEquals(queryJson, gson.toJson(query));
  }

  @Ignore
  public void fullQueryParameters() {
    String user = "xyz";
    Float userBias = 1.0f;
    String item = "53454543513";
    Float itemBias = 1.0f;
    Integer num = 4;
    List<Field> fields = new LinkedList<>();
    String fieldJson = "{\"name\":\"categories\"," +
            "\"values\":[\"series\",\"mini-series\"]," +
            "\"bias\":-1.0}";
    fields.add(gson.fromJson(fieldJson, Field.class));
    DateRange dateRange = new DateRange("dateFieldname", "2015-09-15T11:28:45.114-07:00", "2016-09-15T11:28:45.114-07:00");
    String currentDate = "2015-08-15T11:28:45.114-07:00";
    List<String> blacklistItems = new LinkedList<>();
    blacklistItems.add("itemId1");
    blacklistItems.add("itemId2");
    Boolean returnSelf = false;
    List<String> eventNames = new LinkedList<>();
    eventNames.add("purchase");
    eventNames.add("view");

    Query query = new Query(user,
            userBias,
            item,
            itemBias,
            fields,
            new DateTime(currentDate),
            dateRange,
            blacklistItems,
            returnSelf,
            num,
            eventNames,
            false);

    String queryJson = gson.toJson(query);

    Query queryFromJson = gson.fromJson(queryJson, Query.class);

    assertEquals(query, queryFromJson);
  }
}