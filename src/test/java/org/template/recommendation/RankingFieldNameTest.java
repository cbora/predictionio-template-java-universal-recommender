package org.template.recommendation;

import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class RankingFieldNameTest {
    @Test
    public void toList() throws Exception {
        List<String> list = new LinkedList<>();
        list.add("userRank");
        list.add("uniqueRank");
        list.add("popRank");
        list.add("trendRank");
        list.add("hotRank");

        assertEquals(list, new RankingFieldName().toList());
    }

    @Test
    public void toStringTest() throws Exception {
        String s = "userRank, uniqueRank, popRank, hotRank, unknownRank";

        assertEquals(s, new RankingFieldName().toString());
    }

}