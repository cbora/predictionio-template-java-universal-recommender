package org.template.recommendation;


import org.apache.predictionio.data.storage.DataMap;
import org.apache.predictionio.data.storage.Event;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.json4s.JsonAST;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
public class PreparatorTest {
    private SparkContext sc;

    @Before
    public void setUp() {

        // create spark context
        SparkConf conf = new SparkConf().setAppName("myApp").setMaster("local");
        sc = new SparkContext(conf);



    }



}
