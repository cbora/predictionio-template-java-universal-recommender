package org.template;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;

@AllArgsConstructor
public class Field implements Serializable {
    @Getter private final String name;  // name of metadata field
    @Getter private final List<String> values; // fields can have multiple values
    // like tags of a single value as when using hierarchical taxonomies
    @Getter private final Float bias; // any positive value is a boost, negative is a filter

    @Override
    public String toString() {
        return "Field{" +
                ", name= " + this.name +
                ", values= " + this.values +
                ", bias= " + this.bias +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Field) {
            Field field = (Field) obj;
            return this.name.equals(field.name)
                    && this.values.equals(field.values)
                    && this.bias.equals(field.bias);
        } else {
            return false;
        }
    }
}
