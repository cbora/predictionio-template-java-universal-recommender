package org.template.recommendation;

import org.apache.predictionio.controller.Params;

public class IndicatorParams implements Params {
    private final String name; // must match one in eventNames
    private final Integer maxItemsPerUser; // defaults to maxEventsPerEventType
    private final Integer maxCorrelatorsPerItem; // defaults to maxCorrelatorsPerEventType
    private final Double minLLR; // defaults to none, takes precendence over maxCorrelatorsPerItem


    public IndicatorParams(String name, Integer maxItemsPerUser,
                           Integer maxCorrelatorsPerItem, Double minLLR) {
        this.name = name;
        this.maxItemsPerUser = maxItemsPerUser;
        this.maxCorrelatorsPerItem = maxCorrelatorsPerItem;
        this.minLLR = minLLR;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return "IndicatorParams{" +
                "name: " + this.name +
                "maxItemsPerUser: " + this.maxItemsPerUser +
                "maxCorrelatorsPerItem: " + this.maxCorrelatorsPerItem +
                "minLLR: " + this.minLLR +
                "}";
    }
}
