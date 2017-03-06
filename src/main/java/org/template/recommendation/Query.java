package org.template.recommendation;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

@AllArgsConstructor
public class Query implements Serializable{
    @Getter private final String user; // must be a user or item id
    @Getter private final Float userBias; // default: whatever is in algorithm params or 1
    @Getter private final String item; // must be a user or item id
    @Getter private final Float itemBias; // default: whatever is in algorithm params or 1
    private final List<Field> fields; // default: whatever is in algorithm params or None
    @Getter private final String currentDate; // if used will override dateRange filter, currentDate must lie between the item's
    // expireDateName value and availableDateName value, all are ISO 8601 dates
    @Getter private final DateRange dateRange; // optional before and after filter applied to a date field
    private final List<String> blacklistItems; // default: whatever is in algorithm params or None
    @Getter private final Boolean returnSelf; // means for an item query should the item itself be returned, defaults
    // to what is in the algorithm params or false
    @Getter private final Integer num; // default: whatever is in algorithm params, which itself has a default--probably 20
    private final List<String> eventNames; // names used to ID all user actions
    @Getter private final Boolean withRanks; // Add to ItemScore rank fields values, default false

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

    @Override
    public String toString() {
        return "Query{" +
                "user=' " + this.user +'\'' +
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
}
