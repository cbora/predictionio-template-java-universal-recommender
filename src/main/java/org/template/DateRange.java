package org.template;

import com.google.gson.TypeAdapterFactory;
import lombok.Getter;
import org.apache.predictionio.controller.CustomQuerySerializer;
import org.joda.time.DateTime;
import org.json4s.Formats;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class DateRange implements Serializable, CustomQuerySerializer {
  @Getter private final String name;
  @Getter private final String before; // name of item property for the date comparison
  @Getter private final String after; // both empty should be ignored

  /**
   *  One of the bound can be omitted but not both.
   *  Values for the before and after are strings in ISO 8601 format.
   */
  public DateRange(String name, String before, String after) {

    if ((before == null || before.isEmpty()) && (after == null || after.isEmpty())) {
      throw new IllegalArgumentException("One of the bounds can be omitted but not both");
    }

    this.name = name;

    if (before != null && !before.isEmpty()) {
//      this.before = new DateTime(before);
      this.before = before;
    } else {
      this.before = null;
    }

    if (after != null && !after.isEmpty()) {
//      this.after = new DateTime(after);
      this.after = after;
    } else {
      this.after = null;
    }
  }

  @Override
  public String toString() {
    return "DateRange{" +
            ", name= " + this.name +
            ", before= " + this.before +
            ", after= " + this.after +
            '}';
  }

  @Override
  public Formats querySerializer() {
    return null;
  }

  @Override
  public Seq<TypeAdapterFactory> gsonTypeAdapterFactories() {
    List<TypeAdapterFactory> typeAdapterFactoryList = new LinkedList<>();
    typeAdapterFactoryList.add(new DateRangeTypeAdapterFactory());
    return JavaConversions.asScalaBuffer(typeAdapterFactoryList).toSeq();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DateRange) {
      DateRange dateRange = (DateRange) obj;
      return this.name.equals(dateRange.name)
              && this.before.equals(dateRange.before)
              && this.after.equals(dateRange.after);
    } else {
      return false;
    }
  }
}
