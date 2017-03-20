package org.template.recommendation;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class RankingType {
    public static final String Popular = "popular";
    public static final String Trending = "trending";
    public static final String Hot = "hot";
    public static final String UserDefined = "userDefined";
    public static final String Random = "random";

    public List<String> toList() {
        List<String> list = new LinkedList<>();
        list.addAll(Arrays.asList(new String[]{Popular, Trending, Hot, UserDefined, Random}));
        return list;
    }

    @Override
    public String toString() {
        return String.format("%s, %s, %s, %s, %s", Popular, Trending, Hot, UserDefined, Random);
    }
}
