package org.template.recommendation;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class Query implements Serializable{
    private final String user; // must be a user or item id
    private final float userBias; // default: whatever is in algorithm params or 1
    private String item; // must be a user or item id
    private final float itemBias; // default: whatever is in algorithm params or 1
    private final List<Field> fields; // default: whatever is in algorithm params or None
    private final String currentDate; // if used will override dateRange filter, currentDate must lie between the item's
    // expireDateName value and availableDateName value, all are ISO 8601 dates
    private final DateRange dateRange; // optional before and after filter applied to a date field
    private final List<String> blacklistItems; // default: whatever is in algorithm params or None
    private final boolean returnSelf; // means for an item query should the item itself be returned, defaults
    // to what is in the algorithm params or false
    private int num; // default: whatever is in algorithm params, which itself has a default--probably 20
    private List<String> eventNames; // names used to ID all user actions
    private boolean withRanks; // Add to ItemScore rank fields values, default false

    public Query(String user, float userBias, String item, float itemBias,
                 List<Field> fields, String currentDate, DateRange dateRange,
                 List<String> blacklistItems, boolean returnSelf,
                 int num, List<String> eventNames, boolean withRanks) {
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

    public float getUserBias() {
        return this.userBias;
    }

    public String getItem() {
        return this.item;
    }

    public float getItemBias() {
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

    public boolean isReturnSelf() {
        return this.returnSelf;
    }

    public int getNum() {
        return this.num;
    }

    public List<String> getEventNames() {
        if (this.eventNames == null) {
            return Collections.<String>emptyList();
        } else {
            return eventNames;
        }
    }

    public boolean isWithRanks() {
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
