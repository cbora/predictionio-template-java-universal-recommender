package org.template.recommendation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.predictionio.data.storage.Event;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.json4s.JsonAST;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.*;

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

    @Test
    public void unionAll() throws Exception {
        List<String> list1 = Arrays.asList("a", "b", "c");
        List<String> list2 = Arrays.asList("c", "d", "e");
        List<String> list3 = Arrays.asList();

        JavaSparkContext jsc = new JavaSparkContext(sc);
        JavaRDD<String> rdd1 = jsc.parallelize(list1);
        JavaRDD<String> rdd2 = jsc.parallelize(list2);
        JavaRDD<String> rdd3 = jsc.parallelize(list3);

        List<JavaRDD<String>> rddList = Arrays.asList(rdd1, rdd2, rdd3);
        JavaRDD<String> fullRDD = RDDUtils.unionAll(rddList, sc);
        List<String> fullList = fullRDD.collect();

        assertEquals(6, fullList.size());
        assertTrue(fullList.contains("a"));
        assertTrue(fullList.contains("b"));
        assertTrue(fullList.contains("c"));
        assertTrue(fullList.contains("d"));
        assertTrue(fullList.contains("e"));
    }

    @Test
    public void unionAllPair() throws Exception {
        List<Tuple2<String,Integer>> list1 = Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 1));
        List<Tuple2<String,Integer>> list2 = Arrays.asList(
                new Tuple2<>("a",1),
                new Tuple2<>("a",2),
                new Tuple2<>("c",1));
        List<Tuple2<String,Integer>> list3 = Arrays.asList();

        JavaSparkContext jsc = new JavaSparkContext(sc);
        JavaPairRDD<String,Integer> rdd1 = JavaPairRDD.fromJavaRDD(jsc.parallelize(list1));
        JavaPairRDD<String,Integer> rdd2 = JavaPairRDD.fromJavaRDD(jsc.parallelize(list2));
        JavaPairRDD<String,Integer> rdd3 = JavaPairRDD.fromJavaRDD(jsc.parallelize(list3));

        List<JavaPairRDD<String,Integer>> rddList = Arrays.asList(rdd1, rdd2, rdd3);
        JavaPairRDD<String, Integer> fullRDD = RDDUtils.unionAllPair(rddList, sc);
        List<Tuple2<String,Integer>> fullList = fullRDD.collect();

        assertEquals(5, fullRDD.count());
        assertTrue(fullList.contains(new Tuple2<>("a",1)));
        assertTrue(fullList.contains(new Tuple2<>("a",2)));
        assertTrue(fullList.contains(new Tuple2<>("b",1)));
        assertTrue(fullList.contains(new Tuple2<>("c",1)));
    }

    @Test
    public void combineMapByKey() throws Exception {
        Map<String,Integer> map1 = new HashMap<>(
                ImmutableMap.<String,Integer>builder()
                .put("a", 1)
                .put("b", 1)
                .build()
        );

        Map<String,Integer> map2 = new HashMap<>(
                ImmutableMap.<String,Integer>builder()
                        .put("a", 2)
                        .put("c", 1)
                        .build()
        );

        Map<String,Integer> map3 = new HashMap<>(
                ImmutableMap.<String,Integer>builder()
                        .put("x", 1)
                        .put("y", 1)
                        .build()
        );

        List<Tuple2<String, Map<String, Integer>>> list = Arrays.asList(
                new Tuple2<>("k1", map1),
                new Tuple2<>("k1", map2),
                new Tuple2<>("k2", map3)
        );

        JavaSparkContext jsc = new JavaSparkContext(sc);
        JavaPairRDD<String, Map<String, Integer>> rdd = JavaPairRDD.fromJavaRDD(jsc.parallelize(list));
        JavaPairRDD<String, Map<String, Integer>> combRDD = RDDUtils.combineMapByKey(rdd);
        Map<String, Map<String, Integer>> combMap = combRDD.collectAsMap();

        assertEquals(2, combMap.size());
        assertEquals(3, combMap.get("k1").size());
        assertEquals(2, combMap.get("k2").size());

        assertEquals(2, (long) combMap.get("k1").get("a"));
        assertEquals(1, (long) combMap.get("k1").get("b"));
        assertEquals(1, (long) combMap.get("k1").get("c"));
        assertEquals(1, (long) combMap.get("k2").get("x"));
        assertEquals(1, (long) combMap.get("k2").get("y"));
    }

    @Test
    public void combineCollectionByKey() throws Exception {
        Collection<String> coll1 = Arrays.asList("a", "b", "c");
        Collection<String> coll2 = Arrays.asList("c", "d", "e");
        Collection<String> coll3 = Arrays.asList("x", "y");

        List<Tuple2<String, Collection<String>>> list = Arrays.asList(
                new Tuple2<>("k1", coll1),
                new Tuple2<>("k1", coll2),
                new Tuple2<>("k2", coll3)
        );

        JavaSparkContext jsc = new JavaSparkContext(sc);
        JavaPairRDD<String, Collection<String>> rdd = JavaPairRDD.fromJavaRDD(jsc.parallelize(list));
        JavaPairRDD<String, Collection<String>> combRDD = RDDUtils.combineCollectionByKey(rdd);
        Map<String, Collection<String>> combMap = combRDD.collectAsMap();

        assertEquals(2, combMap.size());
        assertEquals(6, combMap.get("k1").size());
        assertEquals(2, combMap.get("k2").size());

        assertTrue(combMap.get("k1").contains("a"));
        assertTrue(combMap.get("k1").contains("b"));
        assertTrue(combMap.get("k1").contains("c"));
        assertTrue(combMap.get("k1").contains("d"));
        assertTrue(combMap.get("k1").contains("e"));
        assertTrue(combMap.get("k2").contains("x"));
        assertTrue(combMap.get("k2").contains("y"));
    }
}