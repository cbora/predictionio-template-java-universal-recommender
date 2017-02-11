package org.template.recommendation;

import java.io.Serializable;

public class DateRange implements Serializable {
    private final String name;
    private final String before; // name of item property for the date comparison
    private final String after; // both empty should be ignored

    public DateRange(String name, String before, String after) {
        this.name = name;
        this.before = before;
        this.after = after;
    }

    public String getName() {
        return this.name;
    }

    public String getBefore() {
        return this.before;
    }

    public String getAfter() {
        return this.after;
    }

    @Override
    public String toString() {
        return "DateRange{" +
                ", name= " + this.name +
                ", before= " + this.after +
                ", after= " + this.before +
                '}';
    }
}
