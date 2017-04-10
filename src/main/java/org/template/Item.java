package org.template;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Set;

@AllArgsConstructor
public class Item implements Serializable{
    @Getter private final Set<String> categories;
    @Getter private final String entityId;

    @Override
    public String toString() {
        return "Item{" +
                "categories=" + categories +
                ", entityId='" + entityId + '\'' +
                '}';
    }
}
