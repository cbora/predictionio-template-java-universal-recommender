package org.template.recommendation;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class RankingTypeTest {
    @Test
    public void toList() throws Exception {
        List<String> list = new LinkedList<>();
        list.add("popular");
        list.add("trending");
        list.add("hot");
        list.add("userDefined");
        list.add("random");

        assertEquals(list, RankingType.toList());
    }

    @Test
    public void toStringTest() throws Exception {
        String s = "popular, trending, hot, userDefined, random";

        assertEquals(s, RankingType.asString());
    }

}