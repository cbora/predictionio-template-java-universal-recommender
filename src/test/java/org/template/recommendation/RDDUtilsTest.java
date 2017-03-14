package org.template.recommendation;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class RDDUtilsTest {
    private SparkContext sc;

    @Before
    public void setUp() {
        // create spark context
        SparkConf conf = new SparkConf().setAppName("myApp").setMaster("local");
        sc = new SparkContext(conf);
    }

    @Test
    public void getEmptyRDD() throws Exception {
        assertEquals(0, RDDUtils.getEmptyRDD(sc).count());
    }

    @Test
    public void getEmptyPairRDD() throws Exception {
        assertEquals(0, RDDUtils.getEmptyPairRDD(sc).count());
    }

}