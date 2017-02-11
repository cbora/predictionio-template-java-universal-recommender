package org.template.recommendation;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by rhenwood39 on 2/11/17.
 */
public class RankingFieldName {
    private final String UserRank = "userRank";
    private final String UniqueRank = "uniqueRank";
    private final String PopRank = "popRank";
    private final String TrendRank = "trendRank";
    private final String HotRank = "hotRank";
    private final String UnknownRank = "unknownRank";

    public List<String> toList() {
        List<String> list = new LinkedList<>();
        list.addAll(Arrays.asList(new String[] {UserRank, UniqueRank, PopRank, TrendRank, UnknownRank}));
        return list;
    }

    @Override
    public String toString() {
        return String.format("%s, %s, %s, %s, %s, %s", UserRank, UniqueRank, PopRank, TrendRank, HotRank);
    }
}
