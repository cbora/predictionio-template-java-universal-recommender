package org.template.recommendation;

import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.predictionio.data.storage.Event;
import org.apache.predictionio.data.store.java.PJavaEventStore;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import scala.Option;
import scala.Tuple2;
import scala.util.Random;
import scala.reflect.ClassTag;


import java.util.*;

public class PopModel {

    private transient static final Logger logger = LoggerFactory.getLogger(PopModel.class);
    private final JavaPairRDD<String, Map<String, JsonAST.JValue>> fieldsRDD;  // ItemID -> ItemProps
    private final SparkContext sc;

    /**
     *
     * @param fieldsRDD
     * @param sc
     */
    public PopModel(JavaPairRDD<String, Map<String, JsonAST.JValue>> fieldsRDD, SparkContext sc) {
        this.fieldsRDD = fieldsRDD;
        this.sc = sc;
    }

    /**
     * Create random rank for all items
     * @param modelName
     * @param eventNames
     * @param appName
     * @param duration
     * @param offsetDate
     * @return RDD<ItemID, Double>
     */
    public JavaPairRDD<String, Double> calc(String modelName, List<String> eventNames, String appName, Integer duration, String offsetDate) {
        //TODO: implement
        return null;
    }

    /**
     * Create random rank for all items
     * @param appName
     * @param interval
     * @return RDD<ItemID, Double>
     */
    public JavaPairRDD<String, Double> calcRandom(String appName, Interval interval) {
        final JavaRDD<Event> events = eventsRDD(appName, null, interval);
        final JavaRDD<String> actionsRDD = events.map(e -> e.targetEntityId()).filter(s -> s.isDefined()).map(s -> s.get()).distinct();
        final JavaRDD<String> itemsRDD = fieldsRDD.map(t -> t._1());

        Random rand = new Random();
        return actionsRDD.union(itemsRDD).distinct().mapToPair(itemID -> new Tuple2<String, Double>(itemID, rand.nextDouble()));
    }

    /**
     * Creates rank from the number of named events per item for the duration
     * @param appName
     * @param eventNames
     * @param interval
     * @return RDD<ItemID, Double>
     */
    public JavaPairRDD<String, Double> calcPopular(String appName, List<String> eventNames, Interval interval) {
        final JavaRDD<Event> events = eventsRDD(appName, eventNames, interval);
        return events.mapToPair(e -> new Tuple2<String, Integer>(e.targetEntityId().get(), 1))
                .reduceByKey((a,b) -> a + b)
                .mapToPair(t -> new Tuple2<String, Double>(t._1(), (double) t._2()));
    }

    /**
     * Creates a rank for each item by dividing the duration in two and counting named events in both buckets
     * then dividing most recent by less recent. This ranks by change in popularity or velocity of populatiy change.
     * Interval(start, end) end instant is always greater than or equal to the start instant.
     * @param appName
     * @param eventNames
     * @param interval
     * @return RDD<ItemID, Double>
     */
    public JavaPairRDD<String, Double> calcTrending(String appName, List<String> eventNames, Interval interval) {
        logger.info("Current Interval: " + interval + ", " + interval.toDurationMillis());
        long halfInterval = interval.toDurationMillis() / 2;
        Interval olderInterval = new Interval(interval.getStart(), interval.getStart().plus(halfInterval));
        logger.info("Older Interval: " + olderInterval);
        Interval newerInterval = new Interval(interval.getStart().plus(halfInterval), interval.getEnd());
        logger.info("Newer Interval: " + newerInterval);

        // TODO: empty checks
        JavaPairRDD<String, Double> olderPopRDD = calcPopular(appName, eventNames, olderInterval);
        JavaPairRDD<String, Double> newerPopRDD = calcPopular(appName, eventNames, newerInterval);
        return newerPopRDD.join(olderPopRDD)
                .mapToPair(t -> new Tuple2<String, Double>(t._1, t._2._1 - t._2._2));

        /* alt way
        final JavaRDD<Event> events = eventsRDD(appName, eventNames, interval);
        return events.mapToPair(e -> new Tuple2<String, Integer>(e.targetEntityId().get(), 1))
                .reduceByKey((a,b) -> a + b)
                .mapToPair(t -> new Tuple2<String, Double>(t._1(), (double) t._2()));
         */
    }

    /**
     * Creates a rank for each item by divding all events per item into three buckets and calculating the change in
     * velocity over time, in other words the acceleration of popularity change.
     * @param appName
     * @param eventNames
     * @param interval
     * @return RDD<ItemID, Double>
     */
    public JavaPairRDD<String, Double> calcHot(String appName, List<String> eventNames, Interval interval) {
        logger.info("Current Interval: " + interval + ", " + interval.toDurationMillis());
        Interval olderInterval = new Interval(interval.getStart(), interval.getStart().plus(interval.toDurationMillis() / 3));
        logger.info("Older Interval: " + olderInterval);
        Interval middleInterval = new Interval(olderInterval.getEnd(), olderInterval.getEnd().plus(olderInterval.toDurationMillis()));
        logger.info("Middle Interval: " + middleInterval);
        Interval newerInterval = new Interval(middleInterval.getEnd(), interval.getEnd());
        logger.info("Newer Interval: " + newerInterval);

        JavaPairRDD<String, Double> olderPopRDD = calcPopular(appName, eventNames, olderInterval);
        JavaPairRDD<String, Double> middlePopRDD = calcPopular(appName, eventNames, middleInterval);
        JavaPairRDD<String, Double> newerPopRDD = calcPopular(appName, eventNames, newerInterval);

        JavaPairRDD<String, Double> newerVelocity = newerPopRDD.join(middlePopRDD)
                .mapToPair(t -> new Tuple2<String, Double>(t._1, t._2._1 - t._2._2));
        JavaPairRDD<String, Double> olderVelocity = middlePopRDD.join(olderPopRDD)
                .mapToPair(t -> new Tuple2<String, Double>(t._1, t._2._1 - t._2._2));
        return newerVelocity.join(olderVelocity)
                .mapToPair(t -> new Tuple2<String, Double>(t._1, t._2._1 - t._2._2));
    }

    /**
     *
     * @param appName
     * @param eventNames
     * @param interval
     * @return
     */
    public JavaRDD<Event> eventsRDD(String appName, List<String> eventNames, Interval interval) {
        logger.info("PopModel getting eventsRDD for startTime: " + interval.getStart() + " and endTime " + interval.getEnd());
        return PJavaEventStore.find(
                appName, // appName
                OptionHelper.<String>none(), // channelName
                OptionHelper.<DateTime>some(interval.getStart()), // startTime
                OptionHelper.<DateTime>some(interval.getEnd()), // untilTime
                OptionHelper.<String>none(), // entityType
                OptionHelper.<String>none(), //entityID
                eventNames != null ? OptionHelper.<List<String>>some(eventNames) : OptionHelper.<List<String>>none(), // eventNames
                OptionHelper.<Option<String>>none(), // targetEntityType
                OptionHelper.<Option<String>>none(), // targetEntityID
                sc // sparkContext
        ).repartition(sc.defaultParallelism());
    }

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkApp"));
        List<Tuple2<Integer, String>> list1 = Arrays.asList(new Tuple2<Integer, String>(1, "one"));
        List<Tuple2<Integer, String>> list2 = Arrays.asList(new Tuple2<Integer, String>(2, "two"));
        List<Tuple2<Integer, String>> list3 = new LinkedList<>();
        List<Tuple2<Integer, String>> list4 = new LinkedList<>();

        JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(list1);
        JavaPairRDD<Integer, String> rdd2 = sc.parallelizePairs(list2);
        JavaPairRDD<Integer, String> rdd3 = sc.parallelizePairs(list3);
        JavaPairRDD<Integer, String> rdd4 = sc.parallelizePairs(list4);

        System.out.println("rdd1: " + rdd1.isEmpty());
        System.out.println("rdd2: " + rdd1.isEmpty());
        System.out.println("rdd3: " + rdd1.isEmpty());
        System.out.println("rdd4: " + rdd1.isEmpty());

        System.out.println("rdd1 x rdd2: " + rdd1.join(rdd2).isEmpty());
        System.out.println("rdd1 x rdd3: " + rdd1.join(rdd3).isEmpty());
        System.out.println("rdd3 x rdd1: " + rdd3.join(rdd1).isEmpty());
        System.out.println("rdd3 x rdd4: " + rdd3.join(rdd4).isEmpty());

        System.out.println("rdd1: " + rdd1.isEmpty());
        System.out.println("rdd2: " + rdd1.isEmpty());
        System.out.println("rdd3: " + rdd1.isEmpty());
        System.out.println("rdd4: " + rdd1.isEmpty());
    }
}
