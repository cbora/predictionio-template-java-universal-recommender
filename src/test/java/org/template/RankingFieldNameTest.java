package org.template;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RankingFieldNameTest {
    @Test
    public void toList() throws Exception {
        List<String> list = new LinkedList<>();
        list.add("userRank");
        list.add("uniqueRank");
        list.add("popRank");
        list.add("trendRank");
        list.add("hotRank");

        assertEquals(list, RankingFieldName.toList());
    }

    @Test
    public void toStringTest() throws Exception {
        String s = "userRank, uniqueRank, popRank, trendRank, hotRank";

        assertEquals(s, RankingFieldName.asString());
    }

}