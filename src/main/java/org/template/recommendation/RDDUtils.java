package org.template.recommendation;

import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.json4s.JsonAST;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Contains various RDD utility functions
 */
public class RDDUtils {

    /**
     * Generate an empty JavaRDD
     * @param sc spark context
     * @param <T> Type for RDD
     * @return JavaRDD &lt T &gt
     */
    public static <T> JavaRDD<T> getEmptyRDD(SparkContext sc) {
        final JavaSparkContext jsc = new JavaSparkContext(sc);
        final JavaRDD<T> empty = jsc.emptyRDD();
        return jsc.emptyRDD();
    }
    /**
     * Generate an empty JavaPairRDD
     * @param sc spark context
     * @param <K> type of key in key-value pair
     * @param <V> type of value in key-value pair
     * @return JavaPairRDD &lt K,V &gt
     */
    public static <K,V> JavaPairRDD<K,V> getEmptyPairRDD(SparkContext sc) {
        final JavaSparkContext jsc = new JavaSparkContext(sc);
        final JavaRDD<Tuple2<K,V>> empty = jsc.emptyRDD();
        return JavaPairRDD.fromJavaRDD(empty);
    }

    /**
     * Union together all rdd's in a list of rdd's
     * @param rddList list of rdd's
     * @param sc spark context
     * @param <T> rdd type
     * @return union of everything in rddList (return empty rdd if list if empty)
     */
    public static <T> JavaRDD<T> unionAll(List<JavaRDD<T>> rddList, SparkContext sc) {
        final JavaRDD<T> acc = RDDUtils.getEmptyRDD(sc);
        for (JavaRDD<T> rdd : rddList) {
            acc.union(rdd);
        }
        return acc;
    }

    /**
     * Union together all rdd's in a list of rdd's
     * @param rddList list of rdd's
     * @param sc spark context
     * @param <K> rdd key type
     * @param <V> rdd value type
     * @return union of everything in rddList (return empty rdd if list if empty)
     */
    public static <K,V> JavaPairRDD<K,V> unionAllPair(List<JavaPairRDD<K,V>> rddList, SparkContext sc) {
        final JavaPairRDD<K, V> acc = RDDUtils.getEmptyPairRDD(sc);
        for (JavaPairRDD<K, V> rdd : rddList) {
            acc.union(rdd);
        }
        return acc;
    }

    /**
     * Union together all maps that correspond to a given key
     * Equivalent to performing rdd.reduceByKey((map1, map2) -> {map1.putAll(map2); return map1;})
     * @param rdd
     * @param <K1> key type for rdd
     * @param <K2> key type for map
     * @param <V> key type for map
     * @return JavaPairRDD &lt K1, Map &lt K2, V &gt &gt
     */
    public static <K1,K2,V> JavaPairRDD<K1,Map<K2,V>> combineMapByKey(JavaPairRDD<K1,Map<K2,V>> rdd) {
        return rdd.reduceByKey((m1, m2) -> {m1.putAll(m2); return m1;});
    }

    /**
     * Union together all collections that correspond to a given key
     * Equivalent to performing rdd.reduceByKey((coll1, coll2) -> {coll1.addAll(coll2); return coll1;})
     * @param rdd
     * @param <K> key type for rdd
     * @param <T> type of collection
     * @return JavaPairRDD &lt K1, Collection &lt K2, V &gt &gt
     */
    public static <K,T> JavaPairRDD<K,Collection<T>> combineCollectionByKey(JavaPairRDD<K,Collection<T>> rdd) {
        return rdd.reduceByKey((c1, c2) -> {c1.addAll(c2); return c1;});
    }

    public static class PropertyMapConverter implements
            PairFunction<
                    Tuple2<String, PropertyMap>,
                    String,
                    Map<String, JsonAST.JValue>
                    > {
        public Tuple2<String, Map<String, JsonAST.JValue>> call(Tuple2<String, PropertyMap> t){
            Map<String, JsonAST.JValue> Jmap =
                    JavaConverters.mapAsJavaMapConverter(t._2().fields()).asJava();
            return new Tuple2<>(t._1(), Jmap);
        }
    }
}
