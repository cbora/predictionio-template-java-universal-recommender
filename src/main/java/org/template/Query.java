package org.template;

import com.google.gson.TypeAdapterFactory;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.predictionio.controller.CustomQuerySerializer;
import org.joda.time.DateTime;
import org.json4s.Formats;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Class representing a Query in UR
 * Refer to http://actionml.com/docs/ur_queries for more information
 */
@AllArgsConstructor
public class Query implements Serializable, CustomQuerySerializer {
  @Getter
  private final String user; // must be a user or item id
  @Getter
  private final Float userBias; // default: whatever is in algorithm params or 1
  @Getter
  private final String item; // must be a user or item id
  @Getter
  private final Float itemBias; // default: whatever is in algorithm params or 1
  private final List<Field> fields; // default: whatever is in algorithm params or None
  @Getter
  private final DateTime currentDate; // if used will override dateRange filter, currentDate must lie between the item's
  // expireDateName value and availableDateName value, all are ISO 8601 dates
  @Getter
  private final DateRange dateRange; // optional before and after filter applied to a date field
  private final List<String> blacklistItems; // default: whatever is in algorithm params or None
  @Getter
  private final Boolean returnSelf; // means for an item query should the item itself be returned, defaults
  // to what is in the algorithm params or false
  @Getter
  private final Integer num; // default: whatever is in algorithm params, which itself has a default--probably 20
  private final List<String> eventNames; // names used to ID all user actions
  @Getter
  private final Boolean withRanks; // Add to ItemScore rank fields values, default false

  public List<Field> getFields() {
    if (this.fields == null) {
      return Collections.<Field>emptyList();
    } else {
      return this.fields;
    }
  }

  public List<String> getBlacklistItems() {
    if (this.blacklistItems == null) {
      return Collections.<String>emptyList();
    } else {
      return this.blacklistItems;
    }
  }

  public Boolean getRankingsOrElse(Boolean defaultValue) {
    return this.withRanks == null
        ? defaultValue
        : this.getWithRanks();
  }

  public Integer getNumOrElse(Integer defaultValue) {
    return this.num == null
        ? defaultValue
        : this.getNum();
  }

  public List<String> getEventNamesOrElse(List<String> defaultValue) {
    return this.eventNames == null
        ? defaultValue
        : this.getEventNames();
  }

  public List<String> getEventNames() {
    if (this.eventNames == null) {
      return Collections.<String>emptyList();
    } else {
      return eventNames;
    }
  }

  public Float getUserBiasOrElse(Float defaultValue) {
    return this.userBias == null
    ? defaultValue
    : this.getUserBias();
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
  public String toString() {
    return "Query{" +
        "user=' " + this.user + '\'' +
        ", userBias= " + this.userBias +
        ", item= " + this.item +
        ", itemBias= " + this.itemBias +
        ", fields= " + this.fields +
        ", currentDate= " + this.currentDate +
        ", dateRange= " + this.dateRange +
        ", blacklistItems= " + this.blacklistItems +
        ", returnSelf= " + this.returnSelf +
        ", num= " + this.num +
        ", eventNames= " + this.eventNames +
        ", withRanks= " + this.withRanks +
        '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Query) {
      Query query = (Query) obj;
      return query.getUser().equals(this.getUser())
          && query.getUserBias().equals(this.getUserBias())
          && query.getFields().equals(this.getFields())
          && query.getCurrentDate().equals(this.getCurrentDate())
          && query.getDateRange().equals(this.getDateRange())
          && query.getBlacklistItems().equals(this.getBlacklistItems())
          && query.getReturnSelf().equals(this.getReturnSelf())
          && query.getNum().equals(this.getNum())
          && query.getEventNames().equals(this.getEventNames())
          && query.getWithRanks().equals(this.getWithRanks());
    } else {
      return false;
    }
  }
}
