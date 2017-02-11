package org.template.recommendation;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by rhenwood39 on 2/11/17.
 */
public class PopModel {
    private static class RankingFieldName {
        private static final String UserRank = "userRank";
        private static  final String UniqueRank = "uniqueRank";
        private static final String PopRank = "popRank";
        private static final String TrendRank = "trendRank";
        private static final String HotRank = "hotRank";
        private static final String UnknownRank = "unknownRank";

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

    private static class RankingType {
        private static final String Popular = "popular";
        private static final String Trending = "trending";
        private static final String Hot = "hot";
        private static final String UserDefined = "userDefined";
        private static final String Random = "random";

        public List<String> toList() {
            List<String> list = new LinkedList<>();
            list.addAll(Arrays.asList(new String[] {Popular, Trending, Hot, UserDefined, Random}));
            return list;
        }

        @Override
        public String toString() {
            return String.format("%s, %s, %s, %s, %s, %s", Popular, Trending, Hot, UserDefined, Random);
        }
    }

}
