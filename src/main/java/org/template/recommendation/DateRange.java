package org.template.recommendation;

import lombok.Getter;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.IllegalFormatException;
import java.util.regex.Pattern;

public class DateRange implements Serializable {
  @Getter private final String name;
  @Getter private final String before; // name of item property for the date comparison
  @Getter private final String after; // both empty should be ignored

  static final Pattern r8601 = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2})T((\\d{2}):"+
          "(\\d{2}):(\\d{2})\\.(\\d{3}))((\\+|-)(\\d{2}):(\\d{2}))");

  /**
   *  One of the bound can be omitted but not both.
   *  Values for the beforeDate and afterDate are strings in ISO 8601 format.
   */
  public DateRange(String name, String before, String after) {

    if ((before == null || before.isEmpty()) && (after == null || after.isEmpty())) {
      throw new IllegalArgumentException("One of the bounds can be omitted but not both");
    }

    if (before != null && !before.isEmpty() && !r8601.matcher(before).matches()) {
        throw new IllegalArgumentException("beforedate does not conform to ISO 8601 format");
    }

    if (after != null && !after.isEmpty()&& !r8601.matcher(after).matches()) {
        throw new IllegalArgumentException("afterdate does not conform to ISO 8601 format");
    }

    this.name = name;
    this.before = before;
    this.after = after;
  }

  @Override
  public String toString() {
    return "DateRange{" +
            ", name= " + this.name +
            ", before= " + this.before +
            ", after= " + this.after +
            '}';
  }

  public DateTime getBeforeDateTime() {
    return new DateTime(this.before);
  }

  public DateTime getAfterDateTime() {
    return new DateTime(this.after);
  }
}
