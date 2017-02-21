package org.template.recommendation;

import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.predictionio.data.storage.Event;
import org.apache.predictionio.data.store.java.PJavaEventStore;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import scala.Option;
import scala.Tuple2;
import scala.util.Random;


import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
        final JavaRDD<String> itemsRDD = fieldsRDD.keys();
//        final JavaRDD<String> itemsRDD = fieldsRDD.map(new Function<Tuple2<String, Map<String,JsonAST.JValue>>, String>() {
//            public String call(Tuple2<String, Map<String,JsonAST.JValue>> t) {return t._1();}
//        });

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
        //TODO: implement
        return null;
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
        //TODO: implement
        return null;
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
        //TODO: implement
        return null;
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
}
