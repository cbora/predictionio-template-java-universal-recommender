package org.template;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Map;

@AllArgsConstructor
public class ItemScore implements Serializable, Comparable<ItemScore> {
    @Getter private final String itemEntityId;
    @Getter private final double score;
    @Getter private final Map<String, Double> ranks;

    @Override
    public String toString() {
        return "ItemScore{" +
                "itemEntityId='" + itemEntityId + '\'' +
                ", score=" + score +
                ", ranks=" + ranks +
                '}';
    }

    @Override
    public int compareTo(ItemScore o) {
        return Double.valueOf(score).compareTo(o.score);
    }
}
