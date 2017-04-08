package org.template;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *  Unit test for Field class
 */
public class FieldTest {

  private Gson gson;

  @Before
  public void init() {
    this.gson = new Gson();
  }

  @Test
  public void constructorTest() {
    String name = "name";
    List<String> values = new LinkedList<>();
    Float bias = 0.0f;
    Field field = new Field(name, values, bias);
    assertEquals(name, field.getName());
    assertEquals(values, field.getValues());
    assertEquals(bias, field.getBias());
  }

  @Test
  public void serializeTest() {
    String json = "{\"name\":\"categories\"," +
            "\"values\":[\"series\",\"mini-series\"]," +
            "\"bias\":-1.0}";
    Field field = gson.fromJson(json, Field.class);

    List<String> values = new LinkedList<>();
    values.add("series");
    values.add("mini-series");
    assertEquals("categories", field.getName());
    assertEquals(values, field.getValues());
    assertEquals(new Float(-1), field.getBias());
  }

  @Test
  public void serializeEmptyListTest() {
    String json = "{\"name\":\"categories\"," +
            "\"values\":[]," +
            "\"bias\":-1.0}";
    Field field = gson.fromJson(json, Field.class);

    List<String> values = new LinkedList<>();
    assertEquals("categories", field.getName());
    assertEquals(values, field.getValues());
    assertEquals(new Float(-1), field.getBias());
  }

  @Test
  public void deserializeTest() {
    String json = "{\"name\":\"categories\"," +
            "\"values\":[\"series\",\"mini-series\"]," +
            "\"bias\":-1.0}";
    Field field = gson.fromJson(json, Field.class);
    assertEquals(json, gson.toJson(field));
  }

  @Test
  public void deserializeEmptyListTest() {
    String json = "{\"name\":\"categories\"," +
            "\"values\":[]," +
            "\"bias\":-1.0}";
    Field field = gson.fromJson(json, Field.class);
    assertEquals(json, gson.toJson(field));
  }

  @Test
  public void toStringTest() {
    String name = "name";
    List<String> values = new LinkedList<>();
    Float bias = 0.0f;
    Field field = new Field(name, values, bias);
    String result = "Field{" +
            ", name= " + name +
            ", values= " + values.toString() +
            ", bias= " + bias +
            '}';
    assertEquals(result, field.toString());
  }

  @Test
  public void toStringNonEmptyValuesTest() {
    String name = "name";
    List<String> values = new LinkedList<>();
    values.add("value1");
    Float bias = 0.0f;
    Field field = new Field(name, values, bias);
    String result = "Field{" +
            ", name= " + name +
            ", values= " + values.toString() +
            ", bias= " + bias +
            '}';
    assertEquals(result, field.toString());
  }
}
