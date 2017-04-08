package org.template;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class RankingFieldName {
    public static final String UserRank = "userRank";
    public static final String UniqueRank = "uniqueRank";
    public static final String PopRank = "popRank";
    public static final String TrendRank = "trendRank";
    public static final String HotRank = "hotRank";
    public static final String UnknownRank = "unknownRank";

    public static List<String> toList() {
        List<String> list = new LinkedList<>();
        list.addAll(Arrays.asList(new String[]{UserRank, UniqueRank, PopRank, TrendRank, HotRank}));
        return list;
    }

    public static String asString() {
        return String.format("%s, %s, %s, %s, %s", UserRank, UniqueRank, PopRank, TrendRank, HotRank);
    }
}
