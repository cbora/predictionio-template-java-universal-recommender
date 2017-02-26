package org.template.recommendation;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

@AllArgsConstructor
public class DateRange implements Serializable {
    @Getter private final String name;
    @Getter private final String before; // name of item property for the date comparison
    @Getter private final String after; // both empty should be ignored


    @Override
    public String toString() {
        return "DateRange{" +
                ", name= " + this.name +
                ", before= " + this.after +
                ", after= " + this.before +
                '}';
    }
}
