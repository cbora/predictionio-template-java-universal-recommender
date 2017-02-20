package org.template.recommendation;


import java.io.Serializable;
import java.util.List;


public class Field implements Serializable {
    private final String name;  // name of metadata field
    private final List<String> values; // fields can have multiple values
    // like tags of a single value as when using hierarchical taxonomies
    private final Float bias; // any positive value is a boost, negative is a filter

    public Field(String name, List<String> values, Float bias) {
        this.name = name;
        this.values = values;
        this.bias = bias;
    }

    public String getName() {
        return this.name;
    }

    public List<String> getValues() {
        return this.values;
    }

    public Float getBias() {
        return this.bias;
    }

    @Override
    public String toString() {
        return "Field{" +
                ", name= " + this.name +
                ", values= " + this.values +
                ", bias= " + this.bias +
                '}';
    }
}
