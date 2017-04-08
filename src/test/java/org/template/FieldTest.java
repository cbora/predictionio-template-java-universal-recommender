package org.template;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *  Unit test for Field class
 */
public class FieldTest {

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
