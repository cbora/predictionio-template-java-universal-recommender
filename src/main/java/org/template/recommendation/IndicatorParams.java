package org.template.recommendation;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.predictionio.controller.Params;

@AllArgsConstructor
public class IndicatorParams implements Params {
    @Getter private final String name; // must match one in eventNames
    @Getter private final Integer maxItemsPerUser; // defaults to maxEventsPerEventType
    @Getter private final Integer maxCorrelatorsPerItem; // defaults to maxCorrelatorsPerEventType
    @Getter private final Double minLLR; // defaults to none, takes precendence over maxCorrelatorsPerItem

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
