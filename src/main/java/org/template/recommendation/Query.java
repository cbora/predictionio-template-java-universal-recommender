package org.template.recommendation;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class Query implements Serializable{
    private final String user; // must be a user or item id
    private final Float userBias; // default: whatever is in algorithm params or 1
    private final String item; // must be a user or item id
    private final Float itemBias; // default: whatever is in algorithm params or 1
    private final List<Field> fields; // default: whatever is in algorithm params or None
    private final String currentDate; // if used will override dateRange filter, currentDate must lie between the item's
    // expireDateName value and availableDateName value, all are ISO 8601 dates
    private final DateRange dateRange; // optional before and after filter applied to a date field
    private final List<String> blacklistItems; // default: whatever is in algorithm params or None
    private final Boolean returnSelf; // means for an item query should the item itself be returned, defaults
    // to what is in the algorithm params or false
    private final Integer num; // default: whatever is in algorithm params, which itself has a default--probably 20
    private final List<String> eventNames; // names used to ID all user actions
    private final Boolean withRanks; // Add to ItemScore rank fields values, default false

    public Query(String user, Float userBias, String item, Float itemBias,
                 List<Field> fields, String currentDate, DateRange dateRange,
                 List<String> blacklistItems, Boolean returnSelf,
                 Integer num, List<String> eventNames, Boolean withRanks) {
        this.user = user;
        this.userBias = userBias;
        this.item = item;
        this.itemBias = itemBias;
        this.fields = fields;
        this.currentDate = currentDate;
        this.dateRange = dateRange;
        this.blacklistItems = blacklistItems;
        this.returnSelf = returnSelf;
        this.num = num;
        this.eventNames = eventNames;
        this.withRanks = withRanks;
    }

    public String getUser() {
        return this.user;
    }

    public Float getUserBias() {
        return this.userBias;
    }

    public String getItem() {
        return this.item;
    }

    public Float getItemBias() {
        return this.itemBias;
    }

    public List<Field> getFields() {
        if (this.fields == null) {
            return Collections.<Field>emptyList();
        } else {
            return this.fields;
        }
    }

    public String getCurrentDate() {
        return this.currentDate;
    }

    public DateRange getDateRange() {
        return this.dateRange;
    }

    public List<String> getBlacklistItems() {
        if (this.blacklistItems == null) {
            return Collections.<String>emptyList();
        } else {
            return this.blacklistItems;
        }
    }

    public Boolean isReturnSelf() {
        return this.returnSelf;
    }

    public Integer getNum() {
        return this.num;
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

    public Boolean isWithRanks() {
        return this.withRanks;
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
