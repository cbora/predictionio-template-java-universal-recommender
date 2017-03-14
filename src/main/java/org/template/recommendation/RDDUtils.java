package org.template.recommendation;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

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
}
